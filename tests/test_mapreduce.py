#!/usr/bin/env python3
"""
Unit tests for Hadoop MapReduce components (mapper.py and reducer.py).
Tests mapper parsing and reducer aggregation logic using Linux/standard pipes.
"""
import io
import subprocess
import sys
import unittest


class TestMapReduce(unittest.TestCase):

    def test_mapper_valid_input(self):
        """Test mapper emits movieId and rating for standard 4-field TSV input."""
        sample_input = "1\t10\t4.0\t2026-11-10 12:00:00\n1\t20\t3.5\t2026-11-10 12:00:01\n"
        proc = subprocess.run(
            [sys.executable, "mapper.py"],
            input=sample_input,
            capture_output=True,
            text=True,
            check=True
        )
        expected = "10\t4.0\n20\t3.5\n"
        self.assertEqual(proc.stdout, expected)

    def test_mapper_malformed_input(self):
        """Test mapper ignores lines with missing fields or non-numeric ratings."""
        sample_input = "1\t10\tbad_rating\t2026-11-10 12:00:00\nshort_line\n2\t15\t5.0\t2026-11-10 12:00:05\n"
        proc = subprocess.run(
            [sys.executable, "mapper.py"],
            input=sample_input,
            capture_output=True,
            text=True,
            check=True
        )
        expected = "15\t5.0\n"
        self.assertEqual(proc.stdout, expected)

    def test_reducer_single_movie(self):
        """Test reducer correctly aggregates count, total, and average for a single movie."""
        sample_input = "100\t3.0\n100\t4.0\n100\t5.0\n"
        proc = subprocess.run(
            [sys.executable, "reducer.py"],
            input=sample_input,
            capture_output=True,
            text=True,
            check=True
        )
        expected = "100\t3\t12.0\t4.00\n"
        self.assertEqual(proc.stdout, expected)

    def test_reducer_multiple_movies(self):
        """Test reducer correctly calculates aggregates across grouped keys."""
        sample_input = "1\t4.0\n1\t5.0\n2\t2.0\n2\t3.0\n2\t4.0\n"
        proc = subprocess.run(
            [sys.executable, "reducer.py"],
            input=sample_input,
            capture_output=True,
            text=True,
            check=True
        )
        expected = "1\t2\t9.0\t4.50\n2\t3\t9.0\t3.00\n"
        self.assertEqual(proc.stdout, expected)

    def test_end_to_end_pipeline_simulation(self):
        """Test end-to-end mapper | sort | reducer simulation on raw ratings."""
        sample_input = (
            "101\t50\t4.5\t2026-11-10 10:00:00\n"
            "102\t50\t3.5\t2026-11-10 10:00:01\n"
            "103\t10\t5.0\t2026-11-10 10:00:02\n"
        )
        # Run mapper
        map_proc = subprocess.run(
            [sys.executable, "mapper.py"],
            input=sample_input,
            capture_output=True,
            text=True,
            check=True
        )
        # Sort output by key (movieId)
        sorted_lines = "".join(sorted(map_proc.stdout.splitlines(keepends=True)))
        
        # Run reducer
        red_proc = subprocess.run(
            [sys.executable, "reducer.py"],
            input=sorted_lines,
            capture_output=True,
            text=True,
            check=True
        )
        expected = "10\t1\t5.0\t5.00\n50\t2\t8.0\t4.00\n"
        self.assertEqual(red_proc.stdout, expected)


if __name__ == "__main__":
    unittest.main()
