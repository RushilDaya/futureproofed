from typing import Optional
from dataclasses import dataclass

@dataclass
class Measurement:
    seic_code: str
    nrg_bal_code: str
    country_code: str
    year: int
    measurement_value: float
    measurent_unit: str
    standardised_measurement_value: Optional[float]
    standardised_measurement_unit: Optional[str]