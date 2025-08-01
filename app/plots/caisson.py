import plotly.graph_objects as go
import numpy as np
from math import sqrt
from pydantic import BaseModel, Field

# Assuming my_types and other imports are correctly set up in your environment
# For demonstration, I will define dummy types if they are not available.
from app.types import NodesDict, LinesDict, MembersDict, CrossSectionsDict

Vec3 = np.ndarray

# --- Constants ---
PASTEL_PALETTE = ["#E416C1", "#098BF5", "#F3083F", "#2704F0"]
FOUNDATION_COLOR = '#C0C0C0'  # Silver-grey for concrete
CLUSTER_TOL = 3.0 # Default tolerance for clustering nodes

# --- Pydantic Model for Caisson Foundation ---

class PlotModelWithCaisson(BaseModel):
    """Pydantic model defining the parameters for a caisson foundation."""
    CAISSON_WIDTH: float = Field(..., description="Caisson width (along the X-axis)")
    CAISSON_DEPTH: float = Field(..., description="Caisson depth (along the Y-axis)")
    CAISSON_THICKNESS: float = Field(..., description="Caisson thickness or height (along the Z-axis)")
    CLUSTER_TOL: float = Field(default=CLUSTER_TOL, description="Tolerance for grouping support nodes into a single foundation")


# --- Core Geometry and Mesh Functions (Largely Unchanged) ---

def compute_beam_vertices_rect(A: Vec3, B: Vec3, width: float, height: float) -> np.ndarray:
    """Computes the 8 vertices of a rectangular beam prism."""
    v = B - A
    length = np.linalg.norm(v)
    if length == 0:
        raise ValueError("Cannot create a beam with zero length.")
    v_hat = v / length
    axes = [np.array([1,0,0]), np.array([0,1,0]), np.array([0,0,1])]
    helper = min(axes, key=lambda ax: abs(np.dot(v_hat, ax)))
    local_y = np.cross(v_hat, helper)
    local_y /= np.linalg.norm(local_y)
    local_z = np.cross(v_hat, local_y)
    local_z /= np.linalg.norm(local_z)
    local_y *= width / 2.0
    local_z *= height / 2.0
    v0, v1, v2, v3 = A + local_y + local_z, A + local_y - local_z, A - local_y - local_z, A - local_y + local_z
    v4, v5, v6, v7 = B + local_y + local_z, B + local_y - local_z, B - local_y - local_z, B - local_y + local_z
    return np.stack([v0, v1, v2, v3, v4, v5, v6, v7])

def add_beam_mesh(fig: go.Figure, verts: np.ndarray, color: str) -> None:
    """Inserts one rectangular prism into the figure for a beam."""
    quads = [(0,1,2,3), (4,5,6,7), (0,1,5,4), (1,2,6,5), (2,3,7,6), (3,0,4,7)]
    i_list, j_list, k_list = [], [], []
    for a, b, c, d in quads:
        i_list.extend([a, a, a, a]); j_list.extend([b, c, c, d]); k_list.extend([c, b, d, c])
    fig.add_trace(go.Mesh3d(
        x=verts[:,0], y=verts[:,1], z=verts[:,2], i=i_list, j=j_list, k=k_list,
        color=color, flatshading=False, opacity=1.0, hoverinfo="skip",
        lighting=dict(ambient=0.5, diffuse=0.7, specular=0.3, roughness=0.9),
        showscale=False
    ))

# --- Helper Functions for Adding Meshes to Figure ---

def add_mesh_to_fig(fig: go.Figure, verts: np.ndarray, i: list, j: list, k: list, color: str, opacity=1.0) -> None:
    """Generic function to add any 3D mesh to the figure."""
    fig.add_trace(go.Mesh3d(
        x=verts[:, 0], y=verts[:, 1], z=verts[:, 2], i=i, j=j, k=k,
        color=color, opacity=opacity, flatshading=False, hoverinfo="skip",
        lighting=dict(ambient=0.5, diffuse=0.7, specular=0.3, roughness=0.9),
        showscale=False,
    ))

def add_box_mesh(fig: go.Figure, center: Vec3, width: float, depth: float, height: float, color: str) -> None:
    """Creates a mesh for an axis-aligned box (caisson) and adds it to the figure."""
    xc, yc, zc = center
    hw, hd, hh = width / 2.0, depth / 2.0, height / 2.0
    verts = np.array([
        [xc-hw, yc-hd, zc-hh], [xc+hw, yc-hd, zc-hh], [xc+hw, yc+hd, zc-hh], [xc-hw, yc+hd, zc-hh],
        [xc-hw, yc-hd, zc+hh], [xc+hw, yc-hd, zc+hh], [xc+hw, yc+hd, zc+hh], [xc-hw, yc+hd, zc+hh],
    ])
    quads = [(0,1,2,3), (4,5,6,7), (0,1,5,4), (2,3,7,6), (0,3,7,4), (1,2,6,5)]
    i, j, k = [], [], []
    for a, b, c, d in quads:
        i.extend([a, a, a, a]); j.extend([b, c, c, d]); k.extend([c, b, d, c])
    add_mesh_to_fig(fig, verts, i, j, k, color)

# --- Foundation Logic ---

def collect_support_nodes(nodes: NodesDict, z_tol=1e-6) -> dict:
    """Filters for nodes at or near z=0 to be treated as supports."""
    return {n['id']: n for n in nodes.values() if abs(n['z']) < z_tol}

def group_caisson_locations(supports: dict, cluster_tol: float) -> dict:
    """Groups close-proximity support nodes into single caisson locations."""
    remaining_supports = list(supports.values())
    caissons = {}
    caisson_id = 1
    while remaining_supports:
        seed = remaining_supports.pop(0)
        cluster_nodes = [seed]
        # Gather other nodes within the cluster tolerance
        other_supports = [s for s in remaining_supports if sqrt((s['x'] - seed['x'])**2 + (s['y'] - seed['y'])**2) < cluster_tol]
        for support in other_supports:
            cluster_nodes.append(support)
            remaining_supports.remove(support)
        
        # Calculate the centroid of the cluster
        if cluster_nodes:
            cx = sum(n['x'] for n in cluster_nodes) / len(cluster_nodes)
            cy = sum(n['y'] for n in cluster_nodes) / len(cluster_nodes)
            cz = sum(n['z'] for n in cluster_nodes) / len(cluster_nodes)
            caissons[caisson_id] = {'x': cx, 'y': cy, 'z': cz, 'node_ids': [n['id'] for n in cluster_nodes]}
            caisson_id += 1
            
    if not caissons:
        print("Warning: Could not identify any support nodes to place caissons.")
    else:
        print(f"Successfully identified {len(caissons)} caisson location(s).")
    return caissons


# --- Main Plotting Function ---

def plot_3d_with_caissons(
    nodes: NodesDict,
    lines: LinesDict,
    members: MembersDict,
    cross_sections: CrossSectionsDict,
    caissons: dict,
    foundation_params: dict,
) -> go.Figure:
    """
    Plots the full structural model including beams and large rectangular caisson foundations.
    """
    fig = go.Figure()
    
    # 1. Setup Scene and Colors
    x_nodes = [n["x"] for n in nodes.values()]
    y_nodes = [n["y"] for n in nodes.values()]
    z_nodes = [n["z"] for n in nodes.values()]

    cs_ids = sorted({m["cross_section_id"] for m in members.values()})
    color_map = {cs_id: PASTEL_PALETTE[i % len(PASTEL_PALETTE)] for i, cs_id in enumerate(cs_ids)}
    cs_labels = {cs_id: cross_sections[cs_id].get("Description", f"Section {cs_id}") for cs_id in cs_ids}
    
    # 2. Add Invisible Bounding Box for Stable Camera
    caisson_height = foundation_params.get("CAISSON_THICKNESS", 10.0)
    x0, x1 = min(x_nodes), max(x_nodes)
    y0, y1 = min(y_nodes), max(y_nodes)
    z0, z1 = min(z_nodes) - caisson_height, max(z_nodes)
    xc, yc, zc = (x0+x1)/2, (y0+y1)/2, (z0+z1)/2
    max_range = max(x1-x0, y1-y0, z1-z0) * 1.1
    xb = [xc - max_range/2, xc + max_range/2]
    yb = [yc - max_range/2, yc + max_range/2]
    zb = [zc - max_range/2, zc + max_range/2]
    fig.add_trace(go.Scatter3d(x=xb*4, y=sorted(yb*4), z=np.repeat(zb, 4), mode='markers', marker=dict(size=0, color='rgba(0,0,0,0)')))

    # 3. Draw Gantry Structure (Beams and Nodes)
    fig.add_trace(go.Scatter3d(x=x_nodes, y=y_nodes, z=z_nodes, mode="markers", marker=dict(size=3, color="black"), hoverinfo="text", showlegend=False))
    
    for member in members.values():
        line = lines[member["line_id"]]
        ni, nj = nodes[line["Ni"]], nodes[line["Nj"]]
        A = np.array([ni["x"], ni["y"], ni["z"]], dtype=float)
        B = np.array([nj["x"], nj["y"], nj["z"]], dtype=float)
        cs = cross_sections[member["cross_section_id"]]
        verts = compute_beam_vertices_rect(A, B, float(cs["h"]), float(cs["h"]))
        add_beam_mesh(fig, verts, color_map[member["cross_section_id"]])

    # 4. Draw Foundations (Caissons)
    if caissons:
        fp = foundation_params
        for caisson_info in caissons.values():
            # The top of the caisson is at z=0. The center for the box mesh is halfway down its thickness.
            caisson_center = np.array([caisson_info['x'], caisson_info['y'], -fp["CAISSON_THICKNESS"] / 2.0])
            add_box_mesh(
                fig,
                center=caisson_center,
                width=fp["CAISSON_WIDTH"],
                depth=fp["CAISSON_DEPTH"],
                height=fp["CAISSON_THICKNESS"],
                color=FOUNDATION_COLOR
            )

    # 5. Finalize Layout and Legend
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


# if __name__ == '__main__':
#     # This block demonstrates how to use the new functions and models.
#     # You would replace this with your actual data loading.

#     # --- Dummy Data Generation for Demonstration ---
#     from pile_cap import get_nodes_lines

#     nodes, lines = get_nodes_lines()

#     from my_types import MembersDict, CrossSectionInfo
#     my_cs = CrossSectionInfo(
#         name = "L70x4",
#         id = 1,
#         A=10,
#         Iz=100,
#         Iy=1000,
#         Jxx=2000,
#         b=0.070,
#         h=0.070
#     )
#     members: MembersDict = {}
#     cs_dict = {1 : my_cs}
#     for line_id in lines:
#         members[line_id] = {"line_id":line_id, "cross_section_id":1, "material_name": "Steel"}


#     # Define a cross-section
#     my_cs = CrossSectionInfo(
#         name = "L70x4",
#         id = 1,
#         A=10,
#         Iz=100,
#         Iy=1000,
#         Jxx=2000,
#         b=0.070,
#         h=0.070
#     )
#     members: MembersDict = {}
#     cs_dict = {1 : my_cs}
#     for line_id in lines:
#         members[line_id] = {"line_id":line_id, "cross_section_id":1, "material_name": "Steel"}

#     # 1. Define foundation parameters using the new Pydantic model
#     foundation_params_model = PlotModelWithCaisson(
#         CAISSON_WIDTH=2,  # Wider than the 10m frame
#         CAISSON_DEPTH=2,   # A narrow, thick slab
#         CAISSON_THICKNESS=5, # "way thicker"
#         CLUSTER_TOL=3.0      # Group supports within 4m of each other
#     )
#     foundation_params_dict = foundation_params_model.model_dump()

#     # 2. Identify support nodes from the model
#     support_nodes = collect_support_nodes(nodes)

#     # 3. Group support nodes into caisson locations
#     caisson_locations = group_caisson_locations(
#         support_nodes,
#         cluster_tol=foundation_params_dict['CLUSTER_TOL']
#     )

#     # 4. Generate the 3D plot with the caisson
#     fig = plot_3d_with_caissons(
#         nodes=nodes,
#         lines=lines,
#         members=members,
#         cross_sections=cs_dict,
#         caissons=caisson_locations,
#         foundation_params=foundation_params_dict
#     )

#     fig.show()