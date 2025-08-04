from math import tan, radians, pi
from typing import Optional, Tuple

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
CONCRETE_COST = 30.0       # currency per m³
EXCAVATION_COST = 20.0     # currency per m³

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


def find_optimal_pile(soil: PileSoilData) -> tuple[ModelWithPiles, float, float, float, float] | None:
    
    best: tuple[ModelWithPiles, float, float, float, float] | None = None
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

            print(f"[DEBUG] D={D}, L={L}, Qult_comp={Qult_comp}, Qult_ten={Qult_ten}, Qallow_comp={Qallow_comp}, Qallow_ten={Qallow_ten}")

            if P_comp > Qallow_comp:
                print(f"[DEBUG] Skipping: P_comp={P_comp} > Qallow_comp={Qallow_comp}")
                continue
            if P_ten > Qallow_ten:
                print(f"[DEBUG] Skipping: P_ten={P_ten} > Qallow_ten={Qallow_ten}")
                continue

            Hallow = broms_capacity(D, L, soil) / FS_LATERAL
            print(f"[DEBUG] D={D}, L={L}, Hallow={Hallow}")

            if H / 4 > Hallow:
                print(f"[DEBUG] Skipping: H/4={H/4} > Hallow={Hallow}")
                continue

            cost = pile_cost(D, L)
            print(f"[DEBUG] Candidate: D={D}, L={L}, cost={cost}")

            if best is None or cost < best[1]:
                best = (
                    ModelWithPiles(PILE_DIAM=D, PILE_LENGTH=L,EDGE_COVER=min([0.3*D, 150]), CAP_THICK=0.9*D, CLUSTER_TOL=3),
                    cost,
                    Hallow,
                    Qallow_comp,
                    Qallow_ten,
                )
                print(f"[DEBUG] New best: cost={cost}, geom={best[0]}")
    print(f"[DEBUG] Best pile geometry: {best}")
    return best
