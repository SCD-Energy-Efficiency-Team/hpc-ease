import tomllib
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
DEFAULT_DATASET_CONFIG = project_root / "dataset.toml"


@dataclass(frozen=True)
class ClusterConfig:
    nodes: int
    cores_per_node: int
    mem_gib_per_node: int


@dataclass(frozen=True)
class DatasetConfig:
    cluster: ClusterConfig
    p_idle_w: float
    p_max_w: float
    carbon_csv: Path
    carbon_column: str
    forecast_horizon_hours: float
    start_epoch: int
    end_epoch: int
    from_date: date

    @property
    def start(self):
        return datetime.fromtimestamp(self.start_epoch, UTC)

    @property
    def end(self):
        return datetime.fromtimestamp(self.end_epoch, UTC)

    @property
    def from_epoch(self):
        return int(datetime(self.from_date.year, self.from_date.month, self.from_date.day, tzinfo=UTC).timestamp())


class DatasetConfigError(Exception):
    """The dataset config is missing, unreadable or wrong; the message says what to fix."""


def _require(table, key, where):
    if key not in table:
        raise DatasetConfigError(f"dataset config: [{where}] has no `{key}`")
    return table[key]


def _number(table, key, where, integer=False):
    value = _require(table, key, where)
    if isinstance(value, bool) or not isinstance(value, int if integer else (int, float)):
        raise DatasetConfigError(f"dataset config: `{where}.{key}` must be {'an integer' if integer else 'a number'}, got {value!r}")
    return value


def _text(table, key, where):
    value = _require(table, key, where)
    if not isinstance(value, str) or not value:
        raise DatasetConfigError(f"dataset config: `{where}.{key}` must be a non-empty string, got {value!r}")
    return value


def _epoch(value, where):
    if isinstance(value, datetime):
        return int((value if value.tzinfo else value.replace(tzinfo=UTC)).timestamp())
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise DatasetConfigError(f"dataset config: `{where}` must be a TOML datetime or an integer epoch, got {value!r}")


def _check_carbon_csv(path, column):
    if not path.is_file():
        raise DatasetConfigError(f"dataset config: `carbon.csv` points at {path}, which does not exist (the path is relative to the repo root)")
    with open(path, encoding="utf-8") as file:
        header = file.readline().strip().split(",")
    if "datetime" not in header:
        raise DatasetConfigError(f"dataset config: {path} has no `datetime` column; columns: {', '.join(header)}")
    if column not in header:
        raise DatasetConfigError(f"dataset config: `carbon.column` is {column!r} but {path} has no such column; columns: {', '.join(header)}")


def load_dataset_config(path=None, check_carbon=False):
    # check_carbon: also open the carbon CSV and confirm the chosen column; only scripts that read carbon data need it
    path = Path(path) if path is not None else DEFAULT_DATASET_CONFIG
    if not path.is_file():
        raise DatasetConfigError(
            f"{path} not found: a dataset config must exist. Create dataset.toml in the repo root "
            "(see dataset.example.toml), or pass --dataset-config PATH."
        )
    try:
        with open(path, "rb") as file:
            raw = tomllib.load(file)
    except tomllib.TOMLDecodeError as error:
        raise DatasetConfigError(f"{path} is not valid TOML: {error}") from error

    cluster = raw.get("cluster", {})
    energy = raw.get("energy", {})
    carbon = raw.get("carbon", {})
    dataset = raw.get("dataset", {})

    from_date = _require(dataset, "from_date", "dataset")
    if not isinstance(from_date, date) or isinstance(from_date, datetime):
        raise DatasetConfigError(f"dataset config: `dataset.from_date` must be a TOML date such as 2025-02-01, got {from_date!r}")

    config = DatasetConfig(
        cluster=ClusterConfig(
            nodes=_number(cluster, "nodes", "cluster", integer=True),
            cores_per_node=_number(cluster, "cores_per_node", "cluster", integer=True),
            mem_gib_per_node=_number(cluster, "mem_gib_per_node", "cluster", integer=True),
        ),
        p_idle_w=_number(energy, "p_idle_w", "energy"),
        p_max_w=_number(energy, "p_max_w", "energy"),
        carbon_csv=project_root / _text(carbon, "csv", "carbon"),
        carbon_column=_text(carbon, "column", "carbon"),
        forecast_horizon_hours=_number(carbon, "forecast_horizon_hours", "carbon"),
        start_epoch=_epoch(_require(dataset, "start", "dataset"), "dataset.start"),
        end_epoch=_epoch(_require(dataset, "end", "dataset"), "dataset.end"),
        from_date=from_date,
    )
    if config.cluster.nodes < 1 or config.cluster.cores_per_node < 1 or config.cluster.mem_gib_per_node < 1:
        raise DatasetConfigError("dataset config: [cluster] values must be positive")
    if config.forecast_horizon_hours <= 0:
        raise DatasetConfigError("dataset config: `carbon.forecast_horizon_hours` must be positive")
    if config.p_idle_w < 0 or config.p_max_w < config.p_idle_w:
        raise DatasetConfigError("dataset config: [energy] needs 0 <= p_idle_w <= p_max_w")
    if config.end_epoch <= config.start_epoch:
        raise DatasetConfigError("dataset config: `dataset.end` must be after `dataset.start`")
    if check_carbon:
        _check_carbon_csv(config.carbon_csv, config.carbon_column)
    return config


def load_dataset_config_or_exit(path=None, check_carbon=False):
    # for scripts that read the config at import time: print the problem and exit instead of a traceback
    try:
        return load_dataset_config(path, check_carbon)
    except DatasetConfigError as error:
        raise SystemExit(f"error: {error}") from error
