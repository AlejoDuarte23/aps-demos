import viktor as vkt
from math import tan, radians, pi
from typing import Optional, Tuple
import json

from pydantic import BaseModel, Field, ConfigDict

class PileSoilData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    gamma: float = Field(..., description="Unit weight γ, kN/m³")
    phi: float = Field(..., description="Friction angle ϕ, degrees")
    c: float = Field(..., description="Undrained cohesion c, kPa (zero for sand)")
    fs: float = Field(..., description="Design skin friction fₛ, kPa")
    qb: float = Field(..., description="Design end bearing q_b, kPa")


class ModelWithPiles(BaseModel):
    PILE_DIAM: float = Field(..., description="Pile diameter")
    PILE_LENGTH: float = Field(..., description="Pile length")
    CAP_THICK: float = Field(..., description="Pile Cap thickness")
    EDGE_COVER: float = Field(..., description="distance between the piles and the cap edge")
    CLUSTER_TOL: float = Field(..., description="Cluter Tolerance default 3m")

class DesignPiles(BaseModel):
    soil: PileSoilData = Field(..., description="Extract the soil information from the context, the inner system will take care of getting the optimal pile  dimension and the applied load, focus on getting the all parameters correctly!")

FS_AXIAL = 2.5
FS_LATERAL = 2.0
CONCRETE_COST = 300     # currency per m³
EXCAVATION_COST = 3450    # currency per m³

def axial_capacity(D: float, L: float, soil: PileSoilData) -> tuple[float, float]:
    As = pi * D * L                   # shaft surface, m²
    Ab = pi * D**2 / 4                # base area, m²
    Qs = As * soil.fs
    Qb = Ab * soil.qb
    return Qs + Qb, Qs                # comp ult, ten ult


def broms_capacity(D: float, L: float, soil: PileSoilData) -> float:
    if soil.c > 0:
        return 2 * soil.c * D * L
    Kp = tan(radians(45 + soil.phi / 2)) ** 2
    return 0.5 * soil.gamma * Kp * D * L**2


def pile_cost(D: float, L: float) -> float:
    vol = pi * D**2 / 4 * L
    
    piles_cost = vol * (CONCRETE_COST + EXCAVATION_COST) * 4   # four piles
    cap_cost = (16*0.9*D)*(CONCRETE_COST + EXCAVATION_COST)
    return piles_cost + cap_cost

def pile_cost_breakdown(D: float, L: float) -> tuple[float, float, float]:
    vol = pi * D**2 / 4 * L
    piles_concrete = vol * CONCRETE_COST * 4
    piles_excavation = vol * EXCAVATION_COST * 4
    cap_concrete = (16*0.9*D) * CONCRETE_COST
    cap_excavation = (16*0.9*D) * EXCAVATION_COST
    concrete_cost = piles_concrete + cap_concrete
    excavation_cost = piles_excavation + cap_excavation
    total_cost = concrete_cost + excavation_cost
    return excavation_cost, concrete_cost, total_cost

def find_optimal_pile(soil: PileSoilData) -> tuple[
    tuple[ModelWithPiles, float, float, float, float], list[dict]
] | None:
    best: tuple[ModelWithPiles, float, float, float, float] | None = None
    iterations: list[dict] = []
    diameters = [0.3, 0.4, 0.5, 0.6, 0.8]
    lengths = [5, 7, 9, 10, 12, 14, 16, 18, 20] 

    P_comp = 10
    P_ten = 5
    H = 5

    for D in diameters:
        for L in lengths:
            Qult_comp, Qult_ten = axial_capacity(D, L, soil)
            Qallow_comp = Qult_comp / FS_AXIAL
            Qallow_ten = Qult_ten / FS_AXIAL

            compliant = True
            if P_comp > Qallow_comp or P_ten > Qallow_ten:
                compliant = False

            Hallow = broms_capacity(D, L, soil) / FS_LATERAL
            if H / 4 > Hallow:
                compliant = False

            excavation_cost, concrete_cost, total_cost = pile_cost_breakdown(D, L)

            iterations.append({
                "Diameter": D,
                "Length": L,
                "ExcavationCost": excavation_cost,
                "ConcreteCost": concrete_cost,
                "TotalCost": total_cost,
                "Compliant": compliant,
                "Qallow_comp": Qallow_comp,
                "Qallow_ten": Qallow_ten,
                "Hallow": Hallow,
            })

            if not compliant:
                continue

            if best is None or total_cost < best[1]:
                best = (
                    ModelWithPiles(PILE_DIAM=D, PILE_LENGTH=L, EDGE_COVER=max([1.3*D, D + 0.150]), CAP_THICK=0.9*D, CLUSTER_TOL=3),
                    total_cost,
                    Hallow,
                    Qallow_comp,
                    Qallow_ten,
                )
    if best is None:
        return None
    return best, iterations

def store_pile_iterations_as_table(pile_iterations: list[dict]):
    """
    Converts a list of pile iteration dicts into a plain table structure
    and stores it as JSON in vkt.Storage.
    Storage key: "piles_iteration"
    """


    if not pile_iterations:
        table_structure = {"headers": [], "data": [], "flags": []}
    else:
        headers = [
            "Diameter (m)",
            "Length (m)",
            "Excavation Cost",
            "Concrete Cost",
            "Total Cost",
        ]
        data = []
        flags = []
        for it in pile_iterations:
            data.append([
                it.get("Diameter"),
                it.get("Length"),
                f"${it.get('ExcavationCost', 0):,.2f}",
                f"${it.get('ConcreteCost', 0):,.2f}",
                f"${it.get('TotalCost', 0):,.2f}",
            ])
            flags.append(bool(it.get("Compliant")))

        table_structure = {
            "headers": headers,
            "data": data,
            "flags": flags,
        }

    vkt.Storage().set(
        "optimization_table",
        data=vkt.File.from_data(
            json.dumps(table_structure).encode("utf-8")
        ),
        scope="entity",
    )