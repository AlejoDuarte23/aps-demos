from pydantic import BaseModel, Field, ConfigDict


class FootingBearingEntry(BaseModel):
    Df: float = Field(..., description="Embedment depth to base, m .Df (m")
    B: float = Field(..., description="Slab width, m  B (m) ")
    qadm: float = Field(..., description="Allowable bearing pressure, qadm (kPa .")


class FootingGeometry(BaseModel):
    # """Geometry and load for a *square* footing (width = length = B)."""
    # model_config = ConfigDict(extra="forbid")
    # B: float = Field(..., gt=0, description="Footing width = length, m")
    # Df: float = Field(..., ge=0, description="Embedment depth to base, m")
    PEDESTAL_WIDTH: float = Field(..., description="Width of the square pedestals.")
    PEDESTAL_HEIGHT: float = Field(..., description="Height of the pedestals from the slab.")
    SLAB_THICK: float = Field(..., description="Thickness of the footing slab.")
    BASE_WIDTH: float = Field(..., description="Total width of the square foundation slab base.")
    CLUSTER_TOL: float = Field(default=3.0, description="Tolerance for grouping support nodes.")

class FootingSoilData(BaseModel):
    model_config = ConfigDict(extra="forbid")
    bearing_table: list[FootingBearingEntry] = Field(..., description="get the values from the context. creaste  list of allowable bearing pressure for combinations of Df and B")
    gamma: float = Field(..., description="Unit weight γ, kN/m³ for Footing design")
    phi: float = Field(..., description="Friction angle φ° for Footing design")
    c: float = Field(..., description="Cohesion c, kPa for Footing design")
    E: float = Field(..., description="Elastic modulus, MPa for Footing design")

class DesignFooting(BaseModel):
    # geometry: FootingGeometry
    soil: FootingSoilData = Field(..., description="Extract the soil information from the context, the inner system will take care of getting the optimal footing dimension and the applied load, focus on getting the allowable bearing table correclty!")

# class PlotFootingModel(BaseModel):
#     PEDESTAL_WIDTH: float = Field(..., description="Width of the square pedestals.")
#     PEDESTAL_HEIGHT: float = Field(..., description="Height of the pedestals from the slab.")
#     SLAB_THICK: float = Field(..., description="Thickness of the footing slab.")
#     BASE_WIDTH: float = Field(..., description="Total width of the square foundation slab base.")
#     CLUSTER_TOL: float = Field(default=3.0, description="Tolerance for grouping support nodes.")

CONCRETE_COST = 30 #M2
COMPACTED_FILL_COST = 15 #M2
EXCAVATION_COST = 20#M2

def calculate_cost(geometry: FootingGeometry) -> float:
    pedestal_volume = geometry.PEDESTAL_WIDTH * geometry.PEDESTAL_WIDTH * geometry.PEDESTAL_HEIGHT
    pedestal_total_volume = pedestal_volume * 4
    slab_volume = geometry.BASE_WIDTH * geometry.BASE_WIDTH * geometry.SLAB_THICK
    fill_volume = geometry.PEDESTAL_HEIGHT * (
        geometry.BASE_WIDTH * geometry.BASE_WIDTH - 4 * (geometry.PEDESTAL_WIDTH * geometry.PEDESTAL_WIDTH)
    )
    # Assume cost is per m3 for concrete, fill, and excavation
    concrete_cost = (pedestal_total_volume + slab_volume) * CONCRETE_COST
    fill_cost = fill_volume * COMPACTED_FILL_COST
    excavation_cost = geometry.BASE_WIDTH * geometry.BASE_WIDTH * (geometry.SLAB_THICK + geometry.PEDESTAL_HEIGHT) * EXCAVATION_COST
    total_cost = concrete_cost + fill_cost + excavation_cost
    return total_cost

def max_bearing_pressure(P: float, M: float, B: float) -> float | None:
    """
    Return the maximum soil pressure [kPa] for a square footing of width B [m]
    loaded by vertical force P [kN] and moment M [kN·m] about the X-axis.

    Case A   | |e| ≤ B / 6     : trapezoidal stress, q_max = (P / A) · (1 + 6·|e| / B)
    Case B   | |e| = B / 6     : triangular stress,  q_max = 2 P / A
    Case C   | B / 6 < |e| < B / 2 : triangular stress on a reduced contact length,
                                      q_max = 2 P / [3 · B · (0.5·B − |e|)]
    Outside  | |e| ≥ B / 2     : uplift over the whole base, return None
    """
    if P <= 0 or B <= 0:
        return None                       # invalid load or geometry
    e = M / P                            # [m]
    A = B ** 2                           # [m²]

    if abs(e) < B / 6:
        return (P / A) * (1 + 6 * abs(e) / B)              # :contentReference[oaicite:0]{index=0}
    if abs(e) == B / 6:
        return 2 * P / A                                    # :contentReference[oaicite:1]{index=1}
    if abs(e) < B / 2:
        return 2 * P / (3 * B * (0.5 * B - abs(e)))         # :contentReference[oaicite:2]{index=2}
    return None                                             # column outside the base



def find_optimal_footing_geometry(soil: FootingSoilData):
    """
    Enumerate candidate geometries and return the cheapest one whose
    maximum bearing pressure does not exceed the allowable value.
    Returns: (optimal_geometry, optimal_cost, acting_soil_pressure)
    """
    best_geom: FootingGeometry | None = None
    best_cost = float("inf")
    best_q_max = None

    pedestal_widths = [0.50, 0.60, 0.70, 0.80]     # [m]
    slab_thicknesses = [0.40, 0.45, 0.50, 0.60]    # [m]

    # design actions – replace by real loads in production
    P_service = 100.0    # [kN]
    M_service = 10.0     # [kN·m]

    for entry in soil.bearing_table:
        for pw in pedestal_widths:
            for st in slab_thicknesses:
                ph = entry.Df - st
                if ph <= 0:
                    continue

                geom = FootingGeometry(
                    PEDESTAL_WIDTH=pw,
                    PEDESTAL_HEIGHT=ph,
                    SLAB_THICK=st,
                    BASE_WIDTH=entry.B,
                    CLUSTER_TOL=3.0
                )

                q_max = max_bearing_pressure(P_service, M_service, geom.BASE_WIDTH)
                if q_max is None or q_max > entry.qadm:
                    continue

                cost = calculate_cost(geom)
                if cost < best_cost:
                    best_cost = cost
                    best_geom = geom
                    best_q_max = q_max

    return best_geom, best_cost, best_q_max