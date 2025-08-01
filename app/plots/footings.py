import plotly.graph_objects as go
import numpy as np
from math import sqrt
from pydantic import BaseModel, Field
from typing import Dict, List, Any, Tuple

# --- Type Definitions (replaces my_types import) ---

Vec3 = np.ndarray
Node = Dict[str, Any]
NodesDict = Dict[int, Node]
Line = Dict[str, Any]
LinesDict = Dict[int, Line]
Member = Dict[str, Any]
MembersDict = Dict[int, Member]
CrossSection = Dict[str, Any]
CrossSectionsDict = Dict[int, CrossSection]

# --- Constants ---

PASTEL_PALETTE = [
    "#E416C1",  # Light Pink
    "#098BF5",  # Baby Blue
    "#F3083F",  # Cotton Candy
    "#2704F0",  # Soft Sky Blue
]
FOUNDATION_COLOR = '#C0C0C0'  # Silver-grey for concrete
CLUSTER_TOL = 3.0

# --- 3D Geometry Computation ---

def compute_beam_vertices_rect(A: Vec3, B: Vec3, width: float, height: float) -> np.ndarray:
    """Computes the 8 vertices of a rectangular beam given start and end points."""
    v = B - A
    length = np.linalg.norm(v)
    if length == 0:
        raise ValueError("Cannot create a beam with zero length.")
    v_hat = v / length

    # Find a vector that is not parallel to v_hat to establish a local frame
    axes = [np.array([1, 0, 0]), np.array([0, 1, 0]), np.array([0, 0, 1])]
    helper = min(axes, key=lambda ax: abs(np.dot(v_hat, ax)))

    # Create a clean local coordinate system
    local_y = np.cross(v_hat, helper)
    local_y /= np.linalg.norm(local_y)
    local_z = np.cross(v_hat, local_y)
    local_z /= np.linalg.norm(local_z)

    local_y *= width / 2.0
    local_z *= height / 2.0

    # Calculate the eight corners of the prism
    v0, v1, v2, v3 = A + local_y + local_z, A + local_y - local_z, A - local_y - local_z, A - local_y + local_z
    v4, v5, v6, v7 = B + local_y + local_z, B + local_y - local_z, B - local_y - local_z, B - local_y + local_z
    return np.stack([v0, v1, v2, v3, v4, v5, v6, v7])

def compute_square_pedestal_mesh(base_center: Vec3, height: float, width: float) -> tuple[np.ndarray, list[int], list[int], list[int]]:
    """Returns vertices and faces for a vertical square pedestal pointing up from the base center."""
    hw = width / 2.0
    # Define the 8 vertices of the pedestal
    verts = np.array([
        [base_center[0] - hw, base_center[1] - hw, base_center[2]],
        [base_center[0] + hw, base_center[1] - hw, base_center[2]],
        [base_center[0] + hw, base_center[1] + hw, base_center[2]],
        [base_center[0] - hw, base_center[1] + hw, base_center[2]],
        [base_center[0] - hw, base_center[1] - hw, base_center[2] + height],
        [base_center[0] + hw, base_center[1] - hw, base_center[2] + height],
        [base_center[0] + hw, base_center[1] + hw, base_center[2] + height],
        [base_center[0] - hw, base_center[1] + hw, base_center[2] + height],
    ])
    # Define the triangles for the mesh
    i, j, k = [], [], []
    quads = [(0, 1, 5, 4), (1, 2, 6, 5), (2, 3, 7, 6), (3, 0, 4, 7), (4, 5, 6, 7), (0, 3, 2, 1)]
    for a, b, c, d in quads:
        i.extend([a, a]); j.extend([b, c]); k.extend([c, d]) # Draw one side of quad
        i.extend([a, c]); j.extend([c, b]); k.extend([d, a]) # Draw back face
    return verts, i, j, k


# --- Mesh Plotting Helpers ---

def add_mesh_to_fig(fig: go.Figure, verts: np.ndarray, i: list, j: list, k: list, color: str, opacity=1.0) -> None:
    """A generic function to add any 3D mesh to the figure."""
    fig.add_trace(go.Mesh3d(
        x=verts[:, 0], y=verts[:, 1], z=verts[:, 2],
        i=i, j=j, k=k,
        color=color, opacity=opacity, flatshading=False, hoverinfo="skip",
        lighting=dict(ambient=0.6, diffuse=0.8, specular=0.4, roughness=0.9),
        showscale=False
    ))

def add_box_mesh(fig: go.Figure, center: Vec3, width: float, depth: float, height: float, color: str) -> None:
    """Creates a mesh for an axis-aligned box (like a slab) and adds it to the figure."""
    xc, yc, zc = center
    hw, hd, hh = width / 2.0, depth / 2.0, height / 2.0
    verts = np.array([
        [xc - hw, yc - hd, zc - hh], [xc + hw, yc - hd, zc - hh], [xc + hw, yc + hd, zc - hh], [xc - hw, yc + hd, zc - hh],
        [xc - hw, yc - hd, zc + hh], [xc + hw, yc - hd, zc + hh], [xc + hw, yc + hd, zc + hh], [xc - hw, yc + hd, zc + hh]
    ])
    i, j, k = [], [], []
    quads = [(0, 1, 2, 3), (4, 5, 6, 7), (0, 1, 5, 4), (2, 3, 7, 6), (0, 3, 7, 4), (1, 2, 6, 5)]
    for a, b, c, d in quads:
        i.extend([a, a]); j.extend([b, c]); k.extend([c, d])
        i.extend([a, c]); j.extend([c, b]); k.extend([d, a])
    add_mesh_to_fig(fig, verts, i, j, k, color)


# --- Data Processing and Grouping ---

def collect_support_nodes(nodes: NodesDict, z_tol: float = 1e-6) -> NodesDict:
    """Filters the node dictionary to return only nodes at or near the ground plane (z=0)."""
    return {nid: n for nid, n in nodes.items() if abs(n['z']) < z_tol}

def group_four_pedestal_sets(supports: NodesDict, cluster_tol: float = CLUSTER_TOL) -> Dict[int, List[Node]]:
    """Groups support nodes into clusters of four to define the locations of footings."""
    remaining, footings, footing_id = list(supports.values()), {}, 1
    while remaining:
        # THE FIX: Removed the trailing comma from the line below.
        seed = remaining.pop(0)
        cluster = [seed]
        # A copy of the list is needed to safely remove items while iterating
        others = list(remaining) 
        for support in others:
            # Now 'seed' is a dictionary as expected, and this line will work.
            dist = sqrt((support['x'] - seed['x'])**2 + (support['y'] - seed['y'])**2)
            if dist < cluster_tol:
                cluster.append(support)
                remaining.remove(support)
        if len(cluster) == 4:
            footings[footing_id] = cluster
            footing_id += 1
    return footings



# --- Main Plotting Function ---

def plot_structure_with_footing(
    nodes: NodesDict,
    lines: LinesDict,
    members: MembersDict,
    cross_sections: CrossSectionsDict,
    footings: Dict[int, List[Node]],
    footing_params: Dict[str, float]
) -> go.Figure:
    """
    Plots the full structural model including beams, pedestals, and the footing slab.
    """
    fig = go.Figure()

    # --- 1. Setup Scene and Colors ---
    x_nodes = [n["x"] for n in nodes.values()]
    y_nodes = [n["y"] for n in nodes.values()]
    z_nodes = [n["z"] for n in nodes.values()]

    cs_ids = sorted({m["cross_section_id"] for m in members.values()})
    color_map = {cs_id: PASTEL_PALETTE[i % len(PASTEL_PALETTE)] for i, cs_id in enumerate(cs_ids)}
    cs_labels = {cs_id: cross_sections[cs_id].get("name", f"Section {cs_id}") for cs_id in cs_ids}

    # --- 2. Add Invisible Bounding Box for Stable Camera ---
    x0, x1 = min(x_nodes), max(x_nodes)
    y0, y1 = min(y_nodes), max(y_nodes)
    z0 = min(z_nodes) - footing_params.get("SLAB_THICK", 1.0)
    z1 = max(z_nodes)
    fig.add_trace(go.Scatter3d(x=[x0, x1], y=[y0, y1], z=[z0, z1], mode='markers', marker=dict(size=0, color='rgba(0,0,0,0)')))

    # --- 3. Draw Structure (Beams and Nodes) ---
    fig.add_trace(go.Scatter3d(x=x_nodes, y=y_nodes, z=z_nodes, mode="markers", marker=dict(size=3, color="black"), showlegend=False))
    
    for member in members.values():
        line = lines[member["line_id"]]
        A = np.array([nodes[line["Ni"]]["x"], nodes[line["Ni"]]["y"], nodes[line["Ni"]]["z"]])
        B = np.array([nodes[line["Nj"]]["x"], nodes[line["Nj"]]["y"], nodes[line["Nj"]]["z"]])
        cs = cross_sections[member["cross_section_id"]]
        verts = compute_beam_vertices_rect(A, B, float(cs["h"]), float(cs["h"]))
        quads = [(0, 1, 2, 3), (4, 5, 6, 7), (0, 1, 5, 4), (1, 2, 6, 5), (2, 3, 7, 6), (3, 0, 4, 7)]
        i, j, k = [], [], []
        for a, b, c, d in quads:
            i.extend([a, a]); j.extend([b, c]); k.extend([c, d])
        add_mesh_to_fig(fig, verts, i, j, k, color_map[member["cross_section_id"]])

    # --- 4. Draw Foundation (Slab and Pedestals) ---
    if footings:
        fp = footing_params
        for footing_id, pedestal_nodes in footings.items():
            xs = [n['x'] for n in pedestal_nodes]
            ys = [n['y'] for n in pedestal_nodes]
            center_x, center_y = sum(xs) / 4.0, sum(ys) / 4.0
            
            # Calculate slab dimensions
            slab_width = (max(xs) - min(xs)) + 2 * fp["EDGE_COVER"]
            slab_depth = (max(ys) - min(ys)) + 2 * fp["EDGE_COVER"]
            z_base = -fp["SLAB_THICK"]

            # Draw the footing slab
            slab_center = np.array([center_x, center_y, z_base + fp["SLAB_THICK"] / 2.0])
            add_box_mesh(fig, slab_center, slab_width, slab_depth, fp["SLAB_THICK"], FOUNDATION_COLOR)
            
            # Draw the individual pedestals on top of the slab
            for node in pedestal_nodes:
                pedestal_base = np.array([node['x'], node['y'], 0]) # Pedestals start at z=0
                verts, i, j, k = compute_square_pedestal_mesh(pedestal_base, fp["PEDESTAL_HEIGHT"], fp["PEDESTAL_WIDTH"])
                add_mesh_to_fig(fig, verts, i, j, k, FOUNDATION_COLOR)

    # --- 5. Finalize Layout and Legend ---
    for cs_id in cs_ids:
        fig.add_trace(go.Scatter3d(x=[None], y=[None], z=[None], mode="markers",
            marker=dict(symbol="square", size=10, color=color_map[cs_id]),
            name=cs_labels[cs_id], showlegend=True))
            
    fig.update_layout(
        scene=dict(
            aspectmode='data', xaxis_visible=False, yaxis_visible=False, zaxis_visible=False,
            bgcolor="white", camera=dict(eye=dict(x=1.5, y=1.5, z=1.5))),
        paper_bgcolor="white", margin=dict(l=0, r=0, t=40, b=0),
        legend=dict(x=0.95, y=0.05, xanchor="right", yanchor="bottom",
            bgcolor="rgba(0,0,0,0)", borderwidth=0, itemsizing="constant", font=dict(size=16, color="black"))
    )

    return fig

# --- Pydantic Models for Data Validation ---

class PlotFootingModel(BaseModel):
    PEDESTAL_WIDTH: float = Field(..., description="Width of the square pedestals.")
    PEDESTAL_HEIGHT: float = Field(..., description="Height of the pedestals from the slab.")
    SLAB_THICK: float = Field(..., description="Thickness of the footing slab.")
    EDGE_COVER: float = Field(..., description="Distance from outer pedestals to the slab edge.")
    CLUSTER_TOL: float = Field(default=3.0, description="Tolerance for grouping support nodes.")

class CrossSectionInfo(BaseModel):
    name: str
    id: int
    A: float
    Iz: float
    Iy: float
    Jxx: float
    b: float
    h: float



# if __name__ == "__main__":

 
    # from pile_cap import get_nodes_lines

    # nodes, lines = get_nodes_lines()

    # from my_types import MembersDict, CrossSectionInfo
    # my_cs = CrossSectionInfo(
    #     name = "L70x4",
    #     id = 1,
    #     A=10,
    #     Iz=100,
    #     Iy=1000,
    #     Jxx=2000,
    #     b=0.070,
    #     h=0.070
    # )
    # members: MembersDict = {}
    # cs_dict = {1 : my_cs}
    # for line_id in lines:
    #     members[line_id] = {"line_id":line_id, "cross_section_id":1, "material_name": "Steel"}

    # footing_params_model = PlotFootingModel(
    #     PEDESTAL_WIDTH=0.6,
    #     PEDESTAL_HEIGHT=2,
    #     SLAB_THICK=0.8,
    #     EDGE_COVER=0.5,
    #     CLUSTER_TOL=6.0  # Increased to capture the 5x5 base
    # )
    # footing_params_dict = footing_params_model.model_dump()

    # support_nodes = collect_support_nodes(nodes)
    # footings = group_four_pedestal_sets(support_nodes, cluster_tol=footing_params_dict['CLUSTER_TOL'])

    # print(f"Identified {len(footings)} footing group(s).")
    # for f_id, f_nodes in footings.items():
    #     print(f"  - Footing {f_id} connects nodes: {[n['id'] for n in f_nodes]}")

    # fig = plot_structure_with_footing(
    #     nodes=nodes,
    #     lines=lines,
    #     members=members,
    #     cross_sections=cs_dict,
    #     footings=footings,
    #     footing_params=footing_params_dict
    # )

    # fig.show()