import math
import ifcopenshell 
import matplotlib.pyplot as plt
from app.types import NodesDict, LinesDict
from collections import defaultdict

def get_nodes_by_z(Nodes: dict, z: float) -> list[int]:
    selected = [node_id for node_id, attrs in Nodes.items() if attrs["z"] == z]
    return selected


def plot_model(nodes: NodesDict, lines: LinesDict) -> None:
    fig = plt.figure(figsize=(7, 5))
    ax = fig.add_subplot(111, projection="3d")

    # Draw nodes
    for nid, data in nodes.items():
        ax.scatter(data["x"], data["y"], data["z"])
        ax.text(data["x"], data["y"], data["z"], f"{nid}", fontsize=8, ha="center")

    # Draw lines
    for line in lines.values():
        ni = nodes[line["Ni"]]
        nj = nodes[line["Nj"]]
        ax.plot([ni["x"], nj["x"]], [ni["y"], nj["y"]], [ni["z"], nj["z"]])

    ax.set_xlabel("X")
    ax.set_ylabel("Y")
    ax.set_zlabel("Z")  # Z is vertical
    ax.set_title("Platform Model / Node IDs and Connectivity")
    plt.tight_layout()
    plt.show()


def get_nodes_lines(file: str) -> tuple[NodesDict, LinesDict]:
    # parse_file = file.decode("utf-8", errors="ignore")
    model = ifcopenshell.file.from_string(file)

    nodes: NodesDict = {}
    for conn in model.by_type("IfcStructuralPointConnection"):
        if not conn.Representation or not conn.Representation.Representations:
            continue
        try:
            node_id = int(conn.Name)
        except Exception:
            # Fallback: use STEP id if Name is missing or not numeric
            node_id = int(conn.id())
        try:
            cart_pt = (
                conn.Representation.Representations[0]
                .Items[0]        # IfcVertexPoint
                .VertexGeometry  # IfcCartesianPoint
            )
            x, y, z = map(float, cart_pt.Coordinates)
        except Exception:
            # If geometry is unexpected, skip this node
            continue
        nodes[node_id] = {"id": node_id, "x": x, "y": y, "z": z}


    ends_by_member: defaultdict[int, list[int]] = defaultdict(list)

    for rel in model.by_type("IfcRelConnectsStructuralMember"):
        mem = rel.RelatingStructuralMember
        # line_id from member.Name if numeric, else fallback to STEP id
        try:
            line_id = int(mem.Name)
        except Exception:
            line_id = int(mem.id())

        conn = rel.RelatedStructuralConnection  # single connection per relation
        try:
            node_id = int(conn.Name)
        except Exception:
            node_id = int(conn.id())

        # keep unique ends in order
        if node_id not in ends_by_member[line_id]:
            ends_by_member[line_id].append(node_id)

    # Build the final lines dict, only for members that have two distinct ends
    lines: LinesDict = {}
    for line_id, ends in ends_by_member.items():
        # remove accidental duplicates while preserving order
        ends = list(dict.fromkeys(ends))
        if len(ends) == 2:
            Ni, Nj = ends[0], ends[1]
            lines[line_id] = {"id": line_id, "Ni": Ni, "Nj": Nj}
    return nodes, lines



def read_nodal_loads(file: str, load_case_name: str = "Tw") -> dict[int, dict[str, float]]:

    model = ifcopenshell.file.from_string(file)
    # map every point connection to its numeric id
    conn_to_id = {}
    for conn in model.by_type("IfcStructuralPointConnection"):
        try:
            conn_to_id[conn] = int(conn.Name)
        except Exception:
            conn_to_id[conn] = int(conn.id())

    # collect all point actions that belong to the load case Tw
    actions = []
    for rel in model.by_type("IfcRelAssignsToGroup"):
        if getattr(rel.RelatingGroup, "Name", "") == load_case_name:
            actions.extend([obj for obj in rel.RelatedObjects
                            if obj.is_a("IfcStructuralPointAction")])

    loads = {}

    for act in actions:
        # --- locate the support node via IfcRelConnectsStructuralActivity ---
        node_id = None
        for rel in act.AssignedToStructuralItem or []:
            item = rel.RelatingElement           # <- THIS is the correct attribute
            if item in conn_to_id:
                node_id = conn_to_id[item]
                break
        if node_id is None:
            continue

        applied = act.AppliedLoad
        if not (applied and applied.is_a("IfcStructuralLoadSingleForce")):
            continue

        fx, fy, fz = (float(applied.ForceX or 0),
                      float(applied.ForceY or 0),
                      float(applied.ForceZ or 0))
        mx, my, mz = (float(applied.MomentX or 0),
                      float(applied.MomentY or 0),
                      float(applied.MomentZ or 0))

        loads[node_id] = {
            "Fx": fx, "Fy": fy, "Fz": fz,
            "Mx": mx, "My": my, "Mz": mz,
            "CSys": act.GlobalOrLocal or "GLOBAL",
            "Magnitude": math.hypot(fx, fy, fz),
        }

    return loads
