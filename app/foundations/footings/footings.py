import math
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
    bearing_table: list[FootingBearingEntry] = Field(..., description="get the values from the context. creaste  list of allowable bearing pressure for combinations of Df and B")
    gamma: float = Field(..., description="Unit weight γ, kN/m³ for Footing design")
    phi: float = Field(..., description="Friction angle φ° for Footing design")
    c: float = Field(..., description="Cohesion c, kPa for Footing design")
    E: float = Field(..., description="Elastic modulus, MPa for Footing design")



# class PlotFootingModel(BaseModel):
#     PEDESTAL_WIDTH: float = Field(..., description="Width of the square pedestals.")
#     PEDESTAL_HEIGHT: float = Field(..., description="Height of the pedestals from the slab.")
#     SLAB_THICK: float = Field(..., description="Thickness of the footing slab.")
#     BASE_WIDTH: float = Field(..., description="Total width of the square foundation slab base.")
#     CLUSTER_TOL: float = Field(default=3.0, description="Tolerance for grouping support nodes.")

CONCRETE_COST = 300 #M3
COMPACTED_FILL_COST = 90 #M3
EXCAVATION_COST = 40#M3

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

def max_bearing_pressure(
    P: float,
    M: float,
    B: float,
    *,
    eps: float = 1e-9,
    min_contact_ratio: float = 0.01  # minimum contact length as a fraction of B (e.g., 1%)
) -> float:
    """
    Maximum soil pressure [kPa] for a square footing of width B [m],
    loaded by vertical force P [kN] and moment M [kN·m] about the X-axis.

    Always returns a finite float. Never returns None or inf.

    Method
    - Standard formulas for |e| < B/2.
    - Near and beyond |e| = B/2, enforce a minimum contact length L_min = min_contact_ratio * B,
      and compute the triangular-pressure peak as if the contact length were L_min:
          q_max = 2P / (3 * B * L_min)
    - Use eps for robust comparisons.
    """
    # Validate inputs
    if not all(map(math.isfinite, (P, M, B))) or P <= 0.0 or B <= 0.0:
        return math.nan

    # Sanitize min_contact_ratio
    # Clamp to a sensible range to avoid zero or overly large contact lengths
    r = max(1e-6, min(0.25, float(min_contact_ratio)))  # up to 25% is safe for this cap
    L_min = r * B

    e = M / P
    ae = abs(e)
    A = B * B

    # Case A: |e| < B/6 → trapezoidal
    if ae < (B / 6.0) - eps:
        return (P / A) * (1.0 + 6.0 * ae / B)

    # Case B: |e| == B/6 within tolerance → triangular over full base
    if abs(ae - (B / 6.0)) <= eps:
        return 2.0 * P / A

    # Case C: B/6 < |e| < B/2 → triangular on reduced contact length
    if ae < (B / 2.0) - eps:
        L = B - 2.0 * ae  # actual contact length
        denom = 3.0 * B * max(L, L_min)  # enforce minimum contact length
        return 2.0 * P / denom

    # Outside kern or at the edge: enforce minimum contact length cap
    # Treat as extreme triangular contact of length L_min.
    return 2.0 * P / (3.0 * B * L_min)                                   # column outside the base



def find_optimal_footing_geometry(soil: FootingSoilData, reactions: dict[int, dict[str, float]]):
    """
    Enumerate candidate geometries and return the cheapest one whose
    maximum bearing pressure does not exceed the allowable value.
    Returns: (optimal_geometry, optimal_cost, acting_soil_pressure, iterations)
    """
    best_geom: FootingGeometry | None = None
    best_cost = float("inf")
    best_q_max = None
    iterations = []

    pedestal_widths = [0.50, 0.60, 0.70, 0.80]     # [m]
    slab_thicknesses = [0.40, 0.45, 0.50, 0.60]    # [m]

    P_service = max([abs(vals["P"]) for vals in reactions.values()])
    Mx = max([abs(vals["Mx"]) for vals in reactions.values()])
    My = max([abs(vals["My"]) for vals in reactions.values()])
    M_service = max([Mx, My])
    print(f"{P_service=}")
    print(f"{M_service=}")
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

                # Calculate self-weight of concrete and fill
                pedestal_volume = geom.PEDESTAL_WIDTH * geom.PEDESTAL_WIDTH * geom.PEDESTAL_HEIGHT * 4
                slab_volume = geom.BASE_WIDTH * geom.BASE_WIDTH * geom.SLAB_THICK
                fill_volume = geom.PEDESTAL_HEIGHT * (
                    geom.BASE_WIDTH * geom.BASE_WIDTH - 4 * (geom.PEDESTAL_WIDTH * geom.PEDESTAL_WIDTH)
                )
                gamma_concrete = 24  # kN/m3 typical
                gamma_fill = soil.gamma  # kN/m3 from soil data

                self_weight_concrete = (pedestal_volume + slab_volume) * gamma_concrete
                self_weight_fill = fill_volume * gamma_fill

                P_total = P_service + self_weight_concrete + self_weight_fill

                q_max = max_bearing_pressure(P_total, M_service, geom.BASE_WIDTH)
                print(f"{q_max=}")
                compliant = q_max is not None and q_max <= entry.qadm

                cost = calculate_cost(geom)
                iterations.append({
                    "PedestalWidth": pw,
                    "SlabThickness": st,
                    "BaseWidth": entry.B,
                    "EmbedmentDepth": entry.Df,
                    "AllowableBearing": entry.qadm,
                    "BearingPressure": q_max,
                    "TotalCost": cost,
                    "Compliant": compliant,
                })

                if not compliant:
                    continue

                if cost < best_cost:
                    best_cost = cost
                    best_geom = geom
                    best_q_max = q_max

    return best_geom, best_cost, best_q_max, iterations

def store_footing_iterations_as_table(footing_iterations: list[dict]):
    import viktor as vkt
    import json

    if not footing_iterations:
        table_structure = {"headers": [], "data": [], "flags": []}
    else:
        # Sort the iterations with compliant options first
        footing_iterations = sorted(footing_iterations, key=lambda x: not x.get("Compliant", False))
        
        headers = [
            "Pedestal Width (m)",
            "Slab Thickness (m)",
            "Base Width (m)",
            "Embedment Depth (m)",
            "Allowable Bearing (kPa)",
            "Bearing Pressure (kPa)",
            "Total Cost",
        ]

        data = []
        flags = []
        for it in footing_iterations:
            data.append([
                it["PedestalWidth"],
                it["SlabThickness"],
                it["BaseWidth"],
                it["EmbedmentDepth"],
                it["AllowableBearing"],
                it["BearingPressure"],
                f"${it.get('TotalCost', 0):,.2f}",
            ])
            flags.append(bool(it.get("Compliant")))

        table_structure = {"headers": headers, "data": data, "flags": flags}

    vkt.Storage().set(
        "optimization_table",
        data=vkt.File.from_data(
            json.dumps(table_structure).encode("utf-8")
        ),
        scope="entity",
    )