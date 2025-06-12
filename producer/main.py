import csv 
import os
import glob
import time
import zstandard as zstd
from io import TextIOWrapper
from quixstreams.sources import Source
from quixstreams import Application
from dotenv import load_dotenv

load_dotenv()


class CSVStreamingSource(Source):

    def __init__(self, name, csv_folder, pattern= "*.zst"):
        super().__init__(name=name)
        self.csv_folder = csv_folder
        self.pattern = pattern

    def run(self):
        """
        Each Source must have a `run` method.

        It will include the logic behind your source, contained within a
        "while self.running" block for exiting when its parent Application stops.

        There a few methods on a Source available for producing to Kafka, like
        `self.serialize` and `self.produce`.
        """
        file_paths = glob.glob(f"{self.csv_folder}/{self.pattern}")
        file_paths.sort()
        print(f"Found {len(file_paths)} zipped CSV files to stream")

        for path in file_paths:
            print("Processing file:", path)
            with open(path, 'rb') as compressed_file:
                dctx = zstd.ZstdDecompressor()
                stream_reader = dctx.stream_reader(compressed_file)
                text_stream = TextIOWrapper(stream_reader,encoding='utf-8')

                reader = csv.DictReader(text_stream)
                for row in reader:
                    if not self.running:
                        return
                    #Stream each row
                    event_serialized = self.serialize(key=row.get("host", "default"), value=row)
                    self.produce(key=event_serialized.key, value=event_serialized.value)
                    print("Produced:", row)
                    time.sleep(1)
        print("All CSV files streamed")




def main():
    """Set up and run the Quix Application that streams CSV data."""

    # Setup necessary objects
    csv_folder = os.environ.get("CSV_FOLDER", "data")

    # Create the Quix application
    app = Application(consumer_group="csv-streamer-group", auto_create_topics=True)

    # Define the output Kafka topic
    output_topic_name = os.environ.get("OUTPUT_TOPIC", "csv-data")
    output_topic = app.topic(output_topic_name)

    # Create the CSV source
    csv_source = CSVStreamingSource(name="csv-producer", csv_folder=csv_folder)

    # Add the source to the app and bind it to the output topic
    app.add_source(source=csv_source, topic=output_topic)

    # Run the streaming application
    app.run()



#  Sources require execution under a conditional main
if __name__ == "__main__":
    main()