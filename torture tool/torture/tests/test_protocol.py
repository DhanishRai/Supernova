"""
Unit tests for the wire protocol encoder, decoder, and data structures.
Standard library unittest only.
"""

import unittest
from torture.protocol import Operation, OpType, ProtocolCodec, StreamDecoder, SubOp


class TestProtocol(unittest.TestCase):

    def test_put_serialization(self) -> None:
        op = Operation.make_put("op-1", "user:100", "alice")
        wire = ProtocolCodec.encode_operation(op)
        self.assertEqual(wire, "put op-1 user:100 alice\n")

    def test_del_serialization(self) -> None:
        op = Operation.make_del("op-2", "user:100")
        wire = ProtocolCodec.encode_operation(op)
        self.assertEqual(wire, "del op-2 user:100\n")

    def test_batch_serialization(self) -> None:
        sub_ops = [
            SubOp(OpType.PUT, "account:A", "90"),
            SubOp(OpType.DEL, "account:B"),
            SubOp(OpType.PUT, "account:C", "110"),
        ]
        op = Operation.make_batch("batch-99", sub_ops)
        wire = ProtocolCodec.encode_operation(op)
        expected = (
            "batch batch-99 3\n"
            "put account:A 90\n"
            "del account:B\n"
            "put account:C 110\n"
        )
        self.assertEqual(wire, expected)

    def test_key_size_limits(self) -> None:
        oversized_key = "k" * 1025
        with self.assertRaises(ValueError):
            Operation.make_put("op-fail", oversized_key, "val")

    def test_value_size_limits(self) -> None:
        oversized_val = "v" * (1_048_576 + 1)
        with self.assertRaises(ValueError):
            Operation.make_put("op-fail", "valid_key", oversized_val)

    def test_batch_limits(self) -> None:
        with self.assertRaises(ValueError):
            Operation.make_batch("empty-batch", [])

        huge_sub_ops = [SubOp(OpType.PUT, f"k{i}", f"v{i}") for i in range(10_001)]
        with self.assertRaises(ValueError):
            Operation.make_batch("overflow-batch", huge_sub_ops)

    def test_parse_response_lines(self) -> None:
        # Valid ACK
        ack = ProtocolCodec.parse_response_line("ACK op-100\n")
        self.assertEqual(ack, ("ACK", "op-100", None))

        # Valid ERR
        err = ProtocolCodec.parse_response_line("ERR op-101 disk full: limit exceeded\n")
        self.assertEqual(err, ("ERR", "op-101", "disk full: limit exceeded"))

        # Ignored / non-protocol lines
        self.assertIsNone(ProtocolCodec.parse_response_line(""))
        self.assertIsNone(ProtocolCodec.parse_response_line("DEBUG: log recovery complete"))

    def test_stream_decoder_torn_and_chunked_reads(self) -> None:
        decoder = StreamDecoder()

        # Chunk 1: Partial ACK
        responses = decoder.feed("ACK op-1")
        self.assertEqual(responses, [])
        self.assertEqual(decoder.get_remaining_unparsed(), "ACK op-1")

        # Chunk 2: Completion of ACK op-1, full ACK op-2, and partial ERR
        responses = decoder.feed("00\nACK op-101\nERR op-1")
        self.assertEqual(len(responses), 2)
        self.assertEqual(responses[0], ("ACK", "op-100", None))
        self.assertEqual(responses[1], ("ACK", "op-101", None))
        self.assertEqual(decoder.get_remaining_unparsed(), "ERR op-1")

        # Chunk 3: Completion of ERR
        responses = decoder.feed("02 out of space\n")
        self.assertEqual(len(responses), 1)
        self.assertEqual(responses[0], ("ERR", "op-102", "out of space"))
        self.assertEqual(decoder.get_remaining_unparsed(), "")


if __name__ == "__main__":
    unittest.main()
