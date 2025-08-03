"""
Foundation Design Toolkit
-------------------------
Three functions – one per foundation type – compute basic safety‑factor
checks using inputs taken straight from your geotechnical report.

*  check_shallow_footing()
*  check_pile_cap_3()        (cap + three piles, Brons axial method)
*  check_monopile()          (Sulzberger embedment method)

Each function receives two *Pydantic‑v2* models:
1.  <Foundation>Geometry – geometry + applied loads
2.  <Foundation>SoilData – parameters pulled from the geo report

Return value → dict with individual safety factors and an overall Boolean.
You can import these helpers into any optimisation loop and iterate over the
geometric variables until every safety factor ≥ 1.0.

Sections of the code include comments such as
    # geo §7  – φ, c, γ
so you know exactly which paragraph/table of the report supplies each value.
"""
from math import sqrt, pi
from typing import Dict
from pydantic import BaseModel, Field, ConfigDict


class FootingGeometry(BaseModel):
    """Geometry and load for a *square* footing (width = length = B)."""
    model_config = ConfigDict(extra="forbid")
    B: float = Field(..., gt=0, description="Footing width = length, m")
    Df: float = Field(..., ge=0, description="Embedment depth to base, m")

class Loads(BaseModel):
    P: float = Field(..., description="Axial load, kN (compression +)")
    Vx: float = Field(0.0, description="Shear in local‑x, kN")
    Vy: float = Field(0.0, description="Shear in local‑y, kN")
    M: float = Field(0.0, description="Bending moment about centre, kN·m")


class FootingBearingEntry(BaseModel):
    Df: float = Field(..., description="Embedment depth to base, m get this value from the context can't be None")
    B: float = Field(..., description="Slab width, m get this value from the context can't be None ")
    q_allow: float = Field(..., description="Allowable bearing pressure, kPa get this value from the context can't be None")


class FootingSoilData(BaseModel):
    model_config = ConfigDict(extra="forbid")
    bearing_table: list[FootingBearingEntry] = Field(..., description="Parse the bearing pressure table and use it for the footing design. the table has allowable bearing pressure for combinations of Df (embedment) and B (square footing slabs base)")
    gamma: float = Field(..., description="Unit weight γ, kN/m³ for Footing design")
    # phi: float = Field(..., description="Friction angle φ° for Footing design")
    # c: float = Field(..., description="Cohesion c, kPa for Footing design")
    # E: float = Field(..., description="Elastic modulus, MPa for Footing design")

class DesignFooting(BaseModel):
    """Combines geometry, loads, and soil data for a footing design."""
    geometry: FootingGeometry
    soil: FootingSoilData

def check_shallow_footing(geo: FootingGeometry, soil: FootingSoilData) -> Dict[str, float]:
    """Return safety factors against bearing and eccentricity for a square footing.

    Bearing check uses classic Meyerhof formulation with eccentricity; settlement
    is approximated with elastic theory and compared to 25 mm by default.
    """
    B = geo.B
    P = geo.P  # kN
    M = geo.M  # kN·m

    # ------ Bearing pressure under combined P & M ------
    e = M / P if P != 0 else 0  # m
    if abs(e) > B / 2:
        raise ValueError("Footing loses full contact – increase B or reduce M.")
    sigma_avg = (P * 1e3) / (B ** 2)  # convert kN to kN/m² ( == kPa )
    sigma_max = sigma_avg * (1 + 6 * abs(e) / B)

    sf_bearing = soil.q_allow / sigma_max

    # ------ Settlement (very rough elastic) ------
    nu = 0.3  # assume
    s_immediate = (sigma_avg * B) / (soil.E * 1e3) * (1 - nu ** 2)  # m
    sf_settlement = 0.025 / s_immediate if s_immediate else float("inf")  # 25 mm limit

    return {
        "SF_bearing": sf_bearing,
        "SF_settlement": sf_settlement,
        "go": sf_bearing >= 1.0 and sf_settlement >= 1.0,
    }


class PileCapGeometry(BaseModel):
    """Loads act at the cap centre; three identical vertical piles on an isosceles‑triangle pattern."""
    model_config = ConfigDict(extra="forbid")
    pile_diameter: float = Field(..., gt=0, description="Pile diameter, m")
    pile_length: float = Field(..., gt=0, description="Embedded length, m")
    cap_width: float = Field(..., gt=0, description="Cap width, m (for lateral spacing)")
    P: float = Field(..., description="Total axial load on the cap, kN")

class PileSoilData(BaseModel):
    """Parameters pulled from §6 & §7; qc profile simplified as average qc_sand for demo."""
    model_config = ConfigDict(extra="forbid")
    qc_sand: float = Field(..., description="Representative cone resistance for sand layers, MPa")
    gamma: float = Field(..., description="Unit weight γ, kN/m³")
    alpha: float = Field(0.6, description="α for clay – if applicable")
    su_clay: float = Field(0.0, description="Undrained shear su, kPa (enter 0 if no clay)")
    fs_axial: float = Field(2.0, description="Factor of safety axial")


def _brons_side_resistance(qc: float, pile_circ: float, length: float) -> float:
    """Very simplified Brons: f_s = 0.02 qc (kPa). Return Q_s (kN)."""
    fs_kpa = 0.02 * qc * 1e3  # convert MPa→kPa
    return fs_kpa * pile_circ * length / 1e3  # kN


def _brons_base_resistance(qc: float, pile_area: float) -> float:
    qb_kpa = 0.6 * qc * 1e3  # empirical
    return qb_kpa * pile_area / 1e3  # kN


def check_pile_cap_4(geo: PileCapGeometry, soil: PileSoilData) -> Dict[str, float]:
    """Axial capacity only  distribute P equally to three piles and apply Brons correlations."""
    r = geo.pile_diameter / 2
    A = pi * r ** 2  # m²
    circum = 2 * pi * r

    Qs = _brons_side_resistance(soil.qc_sand, circum, geo.pile_length)
    Qb = _brons_base_resistance(soil.qc_sand, A)

    Q_ult = Qs + Qb
    Q_allow = Q_ult / soil.fs_axial

    demand = geo.P / 4  # per pile
    sf_axial = Q_allow / demand if demand else float("inf")

    return {
        "SF_axial": sf_axial,
        "go": sf_axial >= 1.0,
    }


class MonopileGeometry(BaseModel):
    model_config = ConfigDict(extra="forbid")
    D: float = Field(..., gt=0, description="Pile outer diameter, m")
    L: float = Field(..., gt=0, description="Actual embedment length below GL, m")
    H: float = Field(..., description="Horizontal shear (√V1²+V2²), kN")
    M: float = Field(..., description="Moment at ground level, kN·m")

class MonopileSoilData(BaseModel):
    model_config = ConfigDict(extra="forbid")
    nh: float = Field(..., description="n_h coefficient for sand, (kN/m³)/m  – §7 Kv‑Kh")
    gamma: float = Field(..., description="Unit weight γ, kN/m³")
    theta_allow: float = Field(0.0044, description="Rotation limit, rad ≈ 0.25°")


def check_monopile(geo: MonopileGeometry, soil: MonopileSoilData) -> Dict[str, float]:
    """Sulzberger closed‑form – sand only – returns SF as L / L_required."""
    D = geo.D
    He = geo.H
    Me = geo.M + He * 0.67 * D  # equivalent overturning moment

    z0 = (4 * Me / (soil.gamma * soil.nh * (D ** 3))) ** (1 / 3)
    L_required = 2.5 * z0

    sf_length = geo.L / L_required

    # head rotation from linear‑depth spring (sand)
    theta = 2 * He / (3 * soil.nh * soil.gamma * (D ** 3) * (z0 ** 2))
    sf_rotation = soil.theta_allow / theta if theta else float("inf")

    return {
        "SF_length": sf_length,
        "SF_rotation": sf_rotation,
        "go": sf_length >= 1.0 and sf_rotation >= 1.0,
    }


FOUNDATION_CHECKERS = {
    "footing": (FootingGeometry, FootingSoilData, check_shallow_footing),
    "pile_cap_3": (PileCapGeometry, PileSoilData, check_pile_cap_4),
    "monopile": (MonopileGeometry, MonopileSoilData, check_monopile),
}




if __name__ == "__main__":
    # Tiny demo / self‑test
    foot_geo = FootingGeometry(B=4, Df=3, P=3000, M=2500)
    foot_soil = FootingSoilData(bearing_table=[FootingBearingEntry(Df=3, B=4, q_allow=350)], gamma=19, phi=34, c=0, E=30)
    print(check_shallow_footing(foot_geo, foot_soil))

    pile_geo = PileCapGeometry(pile_diameter=0.4, pile_length=22, cap_width=2.5, P=3000)
    pile_soil = PileSoilData(qc_sand=10, gamma=19, su_clay=0)
    print(check_pile_cap_4(pile_geo, pile_soil))

    mono_geo = MonopileGeometry(D=3, L=20, H=400, M=2500)
    mono_soil = MonopileSoilData(nh=30, gamma=19)
    print(check_monopile(mono_geo, mono_soil))
