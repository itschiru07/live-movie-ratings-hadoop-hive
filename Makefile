.PHONY: help setup data start test pipeline dashboard clean

help:
	@echo "Available commands:"
	@echo "  make setup      - Run automated WSL/Ubuntu environment setup"
	@echo "  make data       - Download and unpack MovieLens 100k dataset"
	@echo "  make start      - Start MongoDB, HDFS, and YARN daemons"
	@echo "  make test       - Run unit tests for MapReduce jobs"
	@echo "  make pipeline   - Run one full cycle of the analytics pipeline"
	@echo "  make dashboard  - Launch the Streamlit live dashboard"
	@echo "  make clean      - Reset MongoDB and HDFS data to clean state"

setup:
	bash setup_wsl.sh

data:
	bash download_dataset.sh

start:
	bash start_all.sh

test:
	python3 -m unittest discover -s tests -v

pipeline:
	bash run_pipeline.sh

dashboard:
	python3 -m streamlit run dashboard.py --server.headless true

clean:
	bash clear_data.sh
