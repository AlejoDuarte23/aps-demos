import plotly.graph_objects as go
import numpy as np
from math import sqrt
from app.types import NodesDict, LinesDict, MembersDict, CrossSectionsDict

Vec3 = np.ndarray

PASTEL_PALETTE = [
    "#E416C1",  # Light Pink
    "#098BF5",  # Baby Blue
    "#F3083F",  # Cotton Candy
    "#2704F0",  # Soft Sky Blue
]

FOUNDATION_COLOR = '#C0C0C0' # Silver-grey for concrete
CLUSTER_TOL = 3 

def compute_beam_vertices_rect(A: Vec3, B: Vec3, width: float, height: float) -> np.ndarray:
    v = B - A
    length = np.linalg.norm(v)
    if length == 0:
        raise ValueError("member with zero length")
    v_hat = v / length

    # pick wh|ichever world‑axis is most perpendicular to v_hat
    axes = [np.array([1,0,0]), np.array([0,1,0]), np.array([0,0,1])]
    helper = min(axes, key=lambda ax: abs(np.dot(v_hat, ax)))

    # build a clean 2D frame
    local_y = np.cross(v_hat, helper)
    local_y /= np.linalg.norm(local_y)
    local_z = np.cross(v_hat, local_y)
    local_z /= np.linalg.norm(local_z)

    local_y *= width  / 2.0
    local_z *= height / 2.0

    # eight corners
    v0 = A + local_y + local_z
    v1 = A + local_y - local_z
    v2 = A - local_y - local_z
    v3 = A - local_y + local_z
    v4 = B + local_y + local_z
    v5 = B + local_y - local_z
    v6 = B - local_y - local_z
    v7 = B - local_y + local_z
    return np.stack([v0, v1, v2, v3, v4, v5, v6, v7])

def add_beam_mesh(fig: go.Figure, verts: np.ndarray, color: str) -> None:
    """Insert one rectangular prism into the figure, drawing both sides of each face."""
    # define each face by four verts (a,b,c,d)
    quads = [
        (0, 1, 2, 3),  # face at A
        (4, 5, 6, 7),  # face at B
        (0, 1, 5, 4),
        (1, 2, 6, 5),
        (2, 3, 7, 6),
        (3, 0, 4, 7),
    ]

    i_list, j_list, k_list = [], [], []
    for a, b, c, d in quads:
        # two triangles per quad
        # 1) a→b→c
        i_list.append(a); j_list.append(b); k_list.append(c)
        # 2) a→c→d
        i_list.append(a); j_list.append(c); k_list.append(d)

        # duplicate them reversed so back faces show
        # 3) a→c→b
        i_list.append(a); j_list.append(c); k_list.append(b)
        # 4) a→d→c
        i_list.append(a); j_list.append(d); k_list.append(c)

    fig.add_trace(
        go.Mesh3d(
            x=verts[:, 0],
            y=verts[:, 1],
            z=verts[:, 2],
            i=i_list,
            j=j_list,
            k=k_list,
            color=color,
            # draw both sides, disable flat shading to simplify
            flatshading=False,
            opacity=1.0,
            hoverinfo="skip",
            lighting=dict(ambient=0.5, diffuse=0.7, specular=0.3, roughness=0.9),
            showscale=False,
        )
    )


def compute_cylinder_mesh(
    base_center: Vec3,
    height: float,
    radius: float,
    segments: int = 40
) -> tuple[np.ndarray, list[int], list[int], list[int]]:
    """Return verts and faces for a vertical cylinder pointing down from base_center."""
    theta = np.linspace(0, 2 * np.pi, segments, endpoint=False)
    xs = radius * np.cos(theta)
    ys = radius * np.sin(theta)
    # top circle at z = base_z
    top = np.vstack([base_center + np.array([x, y, 0]) for x, y in zip(xs, ys)])
    # bottom circle at z = base_z - height
    bottom = np.vstack([base_center + np.array([x, y, -height]) for x, y in zip(xs, ys)])
    verts = np.vstack([top, bottom])
    i_list = []
    j_list = []
    k_list = []
    for i in range(segments):
        ni = (i + 1) % segments
        # triangle top→bottom→bottom next
        i_list += [i, i]
        j_list += [i + segments, ni + segments]
        k_list += [ni, ni + segments]
        # reversed for backface
        i_list += [i, i]
        j_list += [ni + segments, i + segments]
        k_list += [i + segments, ni]
    return verts, i_list, j_list, k_list

def compute_cone_mesh(
    base_center: Vec3,
    height: float,
    radius: float,
    segments: int = 16
) -> tuple[np.ndarray, list[int], list[int], list[int]]:
    """Return verts and faces for a downward‐pointing cone at base_center."""
    theta = np.linspace(0, 2 * np.pi, segments, endpoint=False)
    xs = radius * np.cos(theta)
    ys = radius * np.sin(theta)
    # circle at base_center
    base = np.vstack([base_center + np.array([x, y, 0]) for x, y in zip(xs, ys)])
    tip = base_center + np.array([0, 0, -height])
    verts = np.vstack([base, tip])
    tip_idx = len(verts) - 1
    i_list = []
    j_list = []
    k_list = []
    for i in range(segments):
        ni = (i + 1) % segments
        # triangle base[i]→base[ni]→tip
        i_list += [i, i]
        j_list += [ni, tip_idx]
        k_list += [tip_idx, ni]
    return verts, i_list, j_list, k_list

def plot_3d_model(
    nodes: NodesDict,
    lines: LinesDict,
    members: MembersDict,
    cross_sections: CrossSectionsDict,
    load: float = 0.0,
) -> go.Figure:
    """Plot members with pastel colours and add red load arrows."""
    x_nodes = [n["x"] for n in nodes.values()]
    y_nodes = [n["y"] for n in nodes.values()]
    z_nodes = [n["z"] for n in nodes.values()]

    # --- colour map per cross‑section ----------------------------------------
    cs_ids = sorted({m["cross_section_id"] for m in members.values()})
    color_map = {cs_id: PASTEL_PALETTE[i % len(PASTEL_PALETTE)] for i, cs_id in enumerate(cs_ids)}
    cs_labels = {
        cs_id: cross_sections[cs_id].get("Description", f"Section {cross_sections[cs_id]['name']}")
        for cs_id in cs_ids
    }

    fig = go.Figure()

    # This is a trick to stabilize the scene by adding an INVISIBLE bounding box

    x0, x1 = min(x_nodes), max(x_nodes)
    y0, y1 = min(y_nodes), max(y_nodes)
    z0, z1 = min(z_nodes), max(z_nodes)

    # Find the center point of the model
    x_center, y_center, z_center = (x0 + x1) / 2, (y0 + y1) / 2, (z0 + z1) / 2
    # Find the largest dimension of the model
    max_range = max(x1 - x0, y1 - y0, z1 - z0)
    half_range = max_range / 2.0

    # Define the 8 corners of a perfect cube centered around the model
    xb = [x_center - half_range, x_center + half_range]
    yb = [y_center - half_range, y_center + half_range]
    zb = [z_center - half_range, z_center + half_range]

    fig.add_trace(go.Scatter3d(
        x=[xb[0], xb[1], xb[0], xb[1], xb[0], xb[1], xb[0], xb[1]],
        y=[yb[0], yb[0], yb[1], yb[1], yb[0], yb[0], yb[1], yb[1]],
        z=[zb[0], zb[0], zb[0], zb[0], zb[1], zb[1], zb[1], zb[1]],
        mode='markers',
        marker=dict(size=0, color='rgba(0,0,0,0)'), # Make markers invisible
        showlegend=False,
        hoverinfo='none'
    ))

    fig.add_trace(
        go.Scatter3d(
            x=x_nodes, y=y_nodes, z=z_nodes, mode="markers",
            marker=dict(size=3, color="black"), hoverinfo="text", showlegend=False,
        )
    )

    for member in members.values():
        line = lines[member["line_id"]]
        ni, nj = nodes[line["Ni"]], nodes[line["Nj"]]
        A = np.array([ni["x"], ni["y"], ni["z"]], float)
        B = np.array([nj["x"], nj["y"], nj["z"]], float)
        cs = cross_sections[member["cross_section_id"]]
        width, height = float(cs["h"]), float(cs["h"])
        verts = compute_beam_vertices_rect(A, B, width, height)
        add_beam_mesh(fig, verts, color_map[member["cross_section_id"]])

    nodes_with_load = list(
        {node for ln in lines.values() if ln.get("Type") == "Joist" for node in (ln["Ni"], ln["Nj"])}
    )
    if nodes_with_load:
        arrow_height, offset = 400.0, 300.0
        cyl_h, cone_h = 0.8 * arrow_height, 0.2 * arrow_height
        cyl_radius, cone_radius = 0.04 * arrow_height, 0.15 * arrow_height
        for nid in nodes_with_load:
            n = nodes[nid]
            base = np.array([n["x"], n["y"], n["z"]], float) + np.array([0.0, 0.0, arrow_height + offset])
            cyl_verts, ci, cj, ck = compute_cylinder_mesh(base, cyl_h, cyl_radius)
            fig.add_trace(go.Mesh3d(
                x=cyl_verts[:, 0], y=cyl_verts[:, 1], z=cyl_verts[:, 2],
                i=ci, j=cj, k=ck, color="red", opacity=1.0, hoverinfo="skip", showlegend=False
            ))
            cone_base = base + np.array([0.0, 0.0, -cyl_h])
            cone_verts, qi, qj, qk = compute_cone_mesh(cone_base, cone_h, cone_radius)
            fig.add_trace(go.Mesh3d(
                x=cone_verts[:, 0], y=cone_verts[:, 1], z=cone_verts[:, 2],
                i=qi, j=qj, k=qk, color="red", opacity=1.0, hoverinfo="skip", showlegend=False
            ))

    for cs_id in cs_ids:
        fig.add_trace(
            go.Scatter3d(
                x=[None], y=[None], z=[None], mode="markers",
                marker=dict(symbol="square", size=10, color=color_map[cs_id]),
                name=cs_labels[cs_id], hoverinfo="none", showlegend=True
            )
        )

    fig.update_layout(
        scene=dict(
            aspectmode='data',
            xaxis_visible=False,
            yaxis_visible=False,
            zaxis_visible=False,
            bgcolor="white",
            camera=dict(eye=dict(x=1.5, y=1.5, z=1.5)),
        ),
        paper_bgcolor="white",
        margin=dict(l=0, r=0, t=40, b=0),
        legend=dict(
            x=0.95, y=0.05, xanchor="right", yanchor="bottom",
            bgcolor="rgba(0,0,0,0)", borderwidth=0, itemsizing="constant", font=dict(size=16, color="black")
        ),
    )

    return fig

def default_blank_scene()->go.Figure:
    """Fallback scene when there is no Plotly objects to plot!"""
    fig = go.Figure()
    fig.update_layout(
        template=None,
        xaxis=dict(visible=False, showgrid=False, zeroline=False, showline=False),
        yaxis=dict(visible=False, showgrid=False, zeroline=False, showline=False),
        paper_bgcolor='white',
        plot_bgcolor='white',
        margin=dict(l=0, r=0, t=0, b=0),
        autosize=True,
    )
    return fig

# --- Helper Functions for Adding Meshes to Figure ---

def add_mesh_to_fig(fig: go.Figure, verts: np.ndarray, i: list, j: list, k: list, color: str, opacity=1.0) -> None:
    """Generic function to add any 3D mesh to the figure."""
    fig.add_trace(go.Mesh3d(
        x=verts[:, 0], y=verts[:, 1], z=verts[:, 2],
        i=i, j=j, k=k,
        color=color,
        opacity=opacity,
        flatshading=False,
        hoverinfo="skip",
        lighting=dict(ambient=0.5, diffuse=0.7, specular=0.3, roughness=0.9),
        showscale=False,
    ))


def add_box_mesh(fig: go.Figure, center: Vec3, width: float, depth: float, height: float, color: str) -> None:
    """Creates a mesh for an axis-aligned box and adds it to the figure."""
    xc, yc, zc = center
    hw, hd = width / 2.0, depth / 2.0
    verts = np.array([
        [xc - hw, yc - hd, zc - height], [xc + hw, yc - hd, zc - height],
        [xc + hw, yc + hd, zc - height], [xc - hw, yc + hd, zc - height],
        [xc - hw, yc - hd, zc], [xc + hw, yc - hd, zc],
        [xc + hw, yc + hd, zc], [xc - hw, yc + hd, zc],
    ])
    quads = [(0,1,2,3), (4,5,6,7), (0,1,5,4), (2,3,7,6), (0,3,7,4), (1,2,6,5)]
    i, j, k = [], [], []
    for a, b, c, d in quads:
        i.extend([a, a, a, a]); j.extend([b, c, c, d]); k.extend([c, b, d, c])
    add_mesh_to_fig(fig, verts, i, j, k, color)


# --- Main Plotting Function ---

# def collect_support_nodes(nodes, z_tol=1e-6):
#     support_nodes = {n['id']: n for n in nodes.values() if abs(n['z']) < z_tol}
#     return support_nodes

# def group_four_pile_sets(supports, cluster_tol=CLUSTER_TOL):
#     remaining_supports, caps, cap_id = list(supports.values()), {}, 1
#     while remaining_supports:
#         seed = remaining_supports.pop(0)
#         current_cluster = [seed]
#         other_supports = list(remaining_supports)
#         for support in other_supports:
#             dist = sqrt((support['x'] - seed['x'])**2 + (support['y'] - seed['y'])**2)
#             if dist < cluster_tol:
#                 current_cluster.append(support)
#                 remaining_supports.remove(support)
#         if len(current_cluster) == 4:
#             caps[cap_id] = current_cluster
#             cap_id += 1
#         else:
#             print(f"Discarding a cluster with {len(current_cluster)} nodes: {[n['id'] for n in current_cluster]}")
#     if not caps: print("Could not find any valid 4-pile groups.")
#     else: print(f"Successfully identified {len(caps)} pile cap groups.")
#     return caps

def plot_3d_with_foundations(
    nodes: NodesDict,
    lines: LinesDict,
    members: MembersDict,
    cross_sections: CrossSectionsDict,
    caps: dict,
    foundation_params: dict,
    load: float = 0.0,
) -> go.Figure:
    """
    Plots the full structural model including beams and foundations.
    This is the comprehensive replacement for the original `plot_3d_model`.
    """
    fig = go.Figure()
    
    # --- 1. Setup Scene and Colors ---
    x_nodes = [n["x"] for n in nodes.values()]
    y_nodes = [n["y"] for n in nodes.values()]
    z_nodes = [n["z"] for n in nodes.values()]

    cs_ids = sorted({m["cross_section_id"] for m in members.values()})
    color_map = {cs_id: PASTEL_PALETTE[i % len(PASTEL_PALETTE)] for i, cs_id in enumerate(cs_ids)}
    cs_labels = {cs_id: cross_sections[cs_id].get("Description", f"Section {cs_id}") for cs_id in cs_ids}
    
    # --- 2. Add Invisible Bounding Box for Stable Camera ---
    x0, x1 = min(x_nodes), max(x_nodes)
    y0, y1 = min(y_nodes), max(y_nodes)
    z0, z1 = min(z_nodes) - foundation_params.get("PILE_LENGTH", 10.0), max(z_nodes) # Include foundation depth
    xc, yc, zc = (x0+x1)/2, (y0+y1)/2, (z0+z1)/2
    max_range = max(x1-x0, y1-y0, z1-z0)
    xb = [xc - max_range/2, xc + max_range/2]
    yb = [yc - max_range/2, yc + max_range/2]
    zb = [zc - max_range/2, zc + max_range/2]

    fig.add_trace(go.Scatter3d(x=xb*4, y=sorted(yb*4), z=np.repeat(zb, 4), mode='markers', marker=dict(size=0, color='rgba(0,0,0,0)')))

    # --- 3. Draw Gantry Structure (Beams and Nodes) ---
    fig.add_trace(go.Scatter3d(x=x_nodes, y=y_nodes, z=z_nodes, mode="markers", marker=dict(size=3, color="black"), hoverinfo="text", showlegend=False))
    
    for member in members.values():
        line = lines[member["line_id"]]
        ni, nj = nodes[line["Ni"]], nodes[line["Nj"]]
        A = np.array([ni["x"], ni["y"], ni["z"]], dtype=float)
        B = np.array([nj["x"], nj["y"], nj["z"]], dtype=float)
        cs = cross_sections[member["cross_section_id"]]
        verts = compute_beam_vertices_rect(A, B, float(cs["h"]), float(cs["h"]))
        add_beam_mesh(fig, verts, color_map[member["cross_section_id"]])

    # --- 4. Draw Foundations (Pile Caps and Piles) ---
    if caps:
        fp = foundation_params
        for cap_id, pile_nodes in caps.items():
            cx = sum(n['x'] for n in pile_nodes) / 4.0
            cy = sum(n['y'] for n in pile_nodes) / 4.0
            xs = [n['x'] for n in pile_nodes]
            ys = [n['y'] for n in pile_nodes]
            cap_width = (max(xs) - min(xs)) + 2 * fp["EDGE_COVER"]
            cap_depth = (max(ys) - min(ys)) + 2 * fp["EDGE_COVER"]
            z_top = 0.0

            # Draw the pile cap
            cap_center = np.array([cx, cy, z_top])
            add_box_mesh(fig, cap_center, cap_width, cap_depth, fp["CAP_THICK"], FOUNDATION_COLOR)
            
            # Draw the individual piles
            for node in pile_nodes:
                pile_base = np.array([node['x'], node['y'], z_top])
                verts, i, j, k = compute_cylinder_mesh(pile_base, fp["PILE_LENGTH"], fp["PILE_DIAM"] / 2.0)
                add_mesh_to_fig(fig, verts, i, j, k, FOUNDATION_COLOR)

    # --- 5. Draw Optional Load Arrows ---
    nodes_with_load = list({node for ln in lines.values() if ln.get("Type") == "Joist" for node in (ln["Ni"], ln["Nj"])})
    if nodes_with_load and load > 0.0:
        arrow_h, offset = 4.0, 3.0
        cyl_h, cone_h = 0.8 * arrow_h, 0.2 * arrow_h
        cyl_r, cone_r = 0.04 * arrow_h, 0.15 * arrow_h
        for nid in nodes_with_load:
            n = nodes[nid]
            base = np.array([n["x"], n["y"], n["z"]], float) + np.array([0, 0, arrow_h + offset])
            cyl_v, ci, cj, ck = compute_cylinder_mesh(base, cyl_h, cyl_r)
            add_mesh_to_fig(fig, cyl_v, ci, cj, ck, "red")
            cone_base = base + np.array([0, 0, -cyl_h])
            cone_v, qi, qj, qk = compute_cone_mesh(cone_base, cone_h, cone_r)
            add_mesh_to_fig(fig, cone_v, qi, qj, qk, "red")

    # --- 6. Finalize Layout and Legend ---
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

def collect_support_nodes(nodes, z_tol=1e-6):
    support_nodes = {n['id']: n for n in nodes.values() if abs(n['z']) < z_tol}
    return support_nodes

def group_four_pile_sets(supports, cluster_tol=CLUSTER_TOL):
    remaining_supports, caps, cap_id = list(supports.values()), {}, 1
    while remaining_supports:
        seed = remaining_supports.pop(0)
        current_cluster = [seed]
        other_supports = list(remaining_supports)
        for support in other_supports:
            dist = sqrt((support['x'] - seed['x'])**2 + (support['y'] - seed['y'])**2)
            if dist < cluster_tol:
                current_cluster.append(support)
                remaining_supports.remove(support)
        if len(current_cluster) == 4:
            caps[cap_id] = current_cluster
            cap_id += 1
        else:
            print(f"Discarding a cluster with {len(current_cluster)} nodes: {[n['id'] for n in current_cluster]}")
    if not caps: print("Could not find any valid 4-pile groups.")
    else: print(f"Successfully identified {len(caps)} pile cap groups.")
    return caps


# if __name__ == "__main__":


    # from app.types import MembersDict, CrossSectionInfo
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

    # foundation_params_model = PlotModelWithPiles(
    #     PILE_DIAM=0.5,
    #     PILE_LENGTH=7,
    #     CAP_THICK=1.2,
    #     EDGE_COVER=0.4,
    #     CLUSTER_TOL=3.0
    # )
    # foundation_params_dict = foundation_params_model.model_dump()
    # support_nodes = collect_support_nodes(nodes)
    # caps = group_four_pile_sets(support_nodes, cluster_tol=foundation_params_dict['CLUSTER_TOL'])

    # fig = plot_3d_with_foundations(
    #     nodes=nodes,
    #     lines=lines,
    #     members=members,
    #     cross_sections=cs_dict,
    #     caps=caps,
    #     foundation_params=foundation_params_dict
    # )
    # fig.show()

