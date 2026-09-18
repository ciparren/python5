import abc
from typing import Any, Protocol


class DataProcessor(abc.ABC):
    name: str = "Data Processor"

    def __init__(self) -> None:
        self._items: list[tuple[int, str]] = []
        self._total: int = 0

    @abc.abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abc.abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def _store(self, value: str) -> None:
        self._items.append((self._total, value))
        self._total += 1

    def output(self) -> tuple[int, str]:
        return self._items.pop(0)

    def total_processed(self) -> int:
        return self._total

    def remaining(self) -> int:
        return len(self._items)


class NumericProcessor(DataProcessor):
    name = "Numeric Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(isinstance(x, (int, float)) for x in data)
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, list):
            for x in data:
                self._store(str(x))
        else:
            self._store(str(data))


class TextProcessor(DataProcessor):
    name = "Text Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(x, str) for x in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, list):
            for x in data:
                self._store(x)
        else:
            self._store(data)


class LogProcessor(DataProcessor):
    name = "Log Processor"

    def _is_log(self, d: Any) -> bool:
        return (isinstance(d, dict)
                and all(isinstance(k, str) for k in d)
                and all(isinstance(v, str) for v in d.values()))

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            return all(self._is_log(x) for x in data)
        return self._is_log(data)

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        if isinstance(data, list):
            for x in data:
                self._store(": ".join(x.values()))
        else:
            self._store(": ".join(data.values()))


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        values = [v for _, v in data]
        print("CSV Output:\n" + ", ".join(values))


class JSONPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        items = [f'"item_{r}": "{v}"' for r, v in data]
        print("JSON Output:\n{" + ", ".join(items) + "}")


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            handled = False
            for proc in self._processors:
                if proc.validate(element):
                    proc.ingest(element)
                    handled = True
                    break
            if not handled:
                print("DataStream error - Can't process element "
                      f"in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if len(self._processors) == 0:
            print("No processor found, no data")
            return
        for proc in self._processors:
            print(f"{proc.name}: total {proc.total_processed()} "
                  f"items processed, remaining {proc.remaining()} "
                  "on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            extracted: list[tuple[int, str]] = []
            for _ in range(nb):
                if proc.remaining() > 0:
                    extracted.append(proc.output())
            if extracted:
                plugin.process_output(extracted)


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===")
    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Processors...")
    stream.register_processor(NumericProcessor())
    stream.register_processor(TextProcessor())
    stream.register_processor(LogProcessor())

    batch1: list[Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [{"log_level": "WARNING", "log_message":
          "Telnet access! Use ssh instead"},
         {"log_level": "INFO", "log_message":
          "User wil is connected"}],
        42,
        ["Hi", "five"]
    ]

    print(f"Send first batch of data on stream: {batch1}")
    stream.process_stream(batch1)
    stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CSVPlugin()
    stream.output_pipeline(3, csv_plugin)
    stream.print_processors_stats()

    batch2: list[Any] = [
        21,
        ["I love AI", "LLMs are wonderful",
         "Stay healthy"],
        [{"log_level": "ERROR", "log_message":
          "500 server crash"},
         {"log_level": "NOTICE", "log_message":
          "Certificate expires in 10 days"}],
        [32, 42, 64, 84, 128, 168],
        "World hello"
    ]

    print(f"\nSend another batch of data:\n{batch2}")
    stream.process_stream(batch2)
    stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    json_plugin = JSONPlugin()
    stream.output_pipeline(5, json_plugin)
    stream.print_processors_stats()
