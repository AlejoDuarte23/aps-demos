"""
add_footing_with_pedestals.py
--------------------------------------------------
This script adds a combined footing (slab and pedestals) to the GA model,
using lines and nodes extracted from the structural model.
The geometry is based on 4-node support groups.
"""

import ifcopenshell
from pathlib import Path
from collections import defaultdict
from math import sqrt

# --- 1. Configuration ---
STRUCT_IFC = Path(r"app\geometry\Substation_Gantry_Structural.ifc")
GA_IFC_IN  = Path(r"app\geometry\Substation_Gantry_GA.ifc")
GA_IFC_OUT = Path(r"app\geometry\Substation_Gantry_GA_with_footings.ifc")

# Foundation Geometric Parameters
CLUSTER_TOL     = 3.0   # m (max distance to group support nodes)
EDGE_COVER = 0.1

def get_nodes_lines():
    """Extracts structural nodes and lines from the structural IFC model."""
    print(f"Attempting to open structural model: {STRUCT_IFC}")
    model = ifcopenshell.open(STRUCT_IFC.as_posix())
    print("Successfully opened structural model.")

    nodes = {}
    for conn in model.by_type("IfcStructuralPointConnection"):
        rep = conn.Representation
        if not rep or not rep.Representations: continue
        try:
            nid = int(conn.Name) if conn.Name else conn.id()
        except (ValueError, TypeError): nid = conn.id()
        cp = rep.Representations[0].Items[0].VertexGeometry
        x, y, z = map(float, cp.Coordinates)
        nodes[nid] = {'id': nid, 'x': x, 'y': y, 'z': z}
    print(f"Extracted {len(nodes)} structural nodes.")

    ends_by_member = defaultdict(list)
    for rel in model.by_type("IfcRelConnectsStructuralMember"):
        mem, conn = rel.RelatingStructuralMember, rel.RelatedStructuralConnection
        try:
            lid = int(mem.Name) if mem.Name else mem.id()
        except (ValueError, TypeError): lid = mem.id()
        try:
            nid = int(conn.Name) if conn.Name else conn.id()
        except (ValueError, TypeError): nid = conn.id()
        if nid not in ends_by_member[lid]:
            ends_by_member[lid].append(nid)

    lines = {lid: {"id": lid, "Ni": ends[0], "Nj": ends[1]} for lid, ends in ends_by_member.items() if len(ends) == 2}
    print(f"Extracted {len(lines)} structural lines (members).")
    return nodes, lines

def collect_support_nodes(nodes, z_tol=1e-6):
    """Filters for nodes at or near the ground plane (Z=0)."""
    support_nodes = {n['id']: n for n in nodes.values() if abs(n['z']) < z_tol}
    print(f"Found {len(support_nodes)} support nodes at Z=0 (tolerance={z_tol}).")
    return support_nodes

def group_four_node_sets(supports, cluster_tol=CLUSTER_TOL):
    """Groups support nodes into 4-node clusters to form the basis of footings."""
    print(f"Grouping supports into clusters with a tolerance of {cluster_tol}m.")
    remaining_supports, footings, footing_id = list(supports.values()), {}, 1
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
            print(f"Found a valid 4-node footing group (Footing #{footing_id}) with nodes: {[n['id'] for n in current_cluster]}")
            footings[footing_id] = current_cluster
            footing_id += 1
        else:
            print(f"Discarding a cluster with {len(current_cluster)} nodes: {[n['id'] for n in current_cluster]}")
    if not footings: print("Could not find any valid 4-node footing groups.")
    else: print(f"Successfully identified {len(footings)} footing groups.")
    return footings

# ------------------------------------------------------------------
# IFC Helper Functions
# ------------------------------------------------------------------
def rect_profile(mdl, w, d):
    p2d = mdl.create_entity("IfcAxis2Placement2D", mdl.create_entity("IfcCartesianPoint", (0.0, 0.0)), None)
    return mdl.create_entity("IfcRectangleProfileDef", "AREA", None, p2d, w, d)

def extrude(mdl, profile, origin, direction, depth):
    place3d = mdl.create_entity("IfcAxis2Placement3D", origin, None, None)
    return mdl.create_entity("IfcExtrudedAreaSolid", profile, place3d, direction, depth)

def body_rep(mdl, solid):
    ctx = mdl.by_type("IfcGeometricRepresentationContext")[0]
    return mdl.create_entity("IfcShapeRepresentation", ctx, "Body", "SweptSolid", [solid])

def prod_shape(mdl, rep):
    return mdl.create_entity("IfcProductDefinitionShape", None, None, [rep])

# ------------------------------------------------------------------
# Main Routine
# ------------------------------------------------------------------
def update_ifc_model(
    PEDESTAL_WIDTH: float = 0.5,
    PEDESTAL_HEIGHT: float = 0.6,
    SLAB_THICK: float = 0.4,
    SLAB_BASE_SIZE: float = 3
):
    print("Starting script: add_footing_with_pedestals.py")
    try:
        print("--- Phase 1: Data Extraction and Grouping ---")
        nodes, _ = get_nodes_lines()
        supports = collect_support_nodes(nodes)
        footing_groups = group_four_node_sets(supports)

        if not footing_groups:
            print("No footing groups were found. Exiting script.")
            return

        print("--- Phase 2: Creating IFC Geometries ---")
        print(f"Opening General Arrangement model: {GA_IFC_IN}")
        ga_model = ifcopenshell.open(GA_IFC_IN.as_posix())
        
        owner_history, site = ga_model.by_type("IfcOwnerHistory")[0], ga_model.by_type("IfcSite")[0]
        new_guid, neg_z = ifcopenshell.guid.new, ga_model.create_entity("IfcDirection", (0.0, 0.0, -1.0))
        created_elements = []
        
        o_local = ga_model.create_entity("IfcCartesianPoint", (0.0, 0.0, 0.0))
        
        for footing_id, pedestal_nodes in footing_groups.items():
            print(f"--- Processing Footing Group #{footing_id} ---")
            
            # --- 1. Create Pedestals ---
            print(f"Creating {len(pedestal_nodes)} pedestals for Footing #{footing_id}...")
            pedestal_profile = rect_profile(ga_model, PEDESTAL_WIDTH, PEDESTAL_WIDTH)
            
            for i, node in enumerate(pedestal_nodes, start=1):
                o_pedestal_global = ga_model.create_entity("IfcCartesianPoint", (node['x'], node['y'], node['z']))
                place_pedestal_global = ga_model.create_entity("IfcLocalPlacement", None, ga_model.create_entity("IfcAxis2Placement3D", o_pedestal_global, None, None))
                solid_pedestal = extrude(ga_model, pedestal_profile, o_local, neg_z, PEDESTAL_HEIGHT)
                shape_pedestal = prod_shape(ga_model, body_rep(ga_model, solid_pedestal))
                pedestal = ga_model.create_entity("IfcColumn", new_guid(), owner_history, f"Pedestal_{footing_id}_{i}", None, "Pedestal", place_pedestal_global, shape_pedestal, "USERDEFINED")
                created_elements.append(pedestal)

            # --- 2. Create Footing Slab ---
            xs = [n['x'] for n in pedestal_nodes]
            ys = [n['y'] for n in pedestal_nodes]
            
            if SLAB_BASE_SIZE > 0:
                slab_width = SLAB_BASE_SIZE
                slab_depth = SLAB_BASE_SIZE
            else:
                slab_width = (max(xs) - min(xs)) + PEDESTAL_WIDTH + 2 * EDGE_COVER
                slab_depth = (max(ys) - min(ys)) + PEDESTAL_WIDTH + 2 * EDGE_COVER
            
            slab_top_z = min(n['z'] for n in pedestal_nodes) - PEDESTAL_HEIGHT
            cx = sum(xs) / len(xs)
            cy = sum(ys) / len(ys)
            
            print(f"Placing IfcFooting (Slab #{footing_id}) at global coordinate (X={cx:.3f}, Y={cy:.3f}, Z={slab_top_z:.3f})")

            o_slab_global = ga_model.create_entity("IfcCartesianPoint", (cx, cy, slab_top_z))
            place_slab_global = ga_model.create_entity("IfcLocalPlacement", None, ga_model.create_entity("IfcAxis2Placement3D", o_slab_global, None, None))
            slab_profile = rect_profile(ga_model, slab_width, slab_depth)
            solid_slab = extrude(ga_model, slab_profile, o_local, neg_z, SLAB_THICK)
            shape_slab = prod_shape(ga_model, body_rep(ga_model, solid_slab))
            footing_slab = ga_model.create_entity("IfcFooting", new_guid(), owner_history, f"FootingSlab_{footing_id}", None, "Slab", place_slab_global, shape_slab, "FOOTING")
            created_elements.append(footing_slab)

        print(f"--- Finalizing: Total new IFC entities created: {len(created_elements)} ---")
        ga_model.create_entity("IfcRelContainedInSpatialStructure", new_guid(), owner_history, "Elements in Site", None, created_elements, site)
        ga_model.write(GA_IFC_OUT.as_posix())
        print(f"SUCCESS: Footings and pedestals written to {GA_IFC_OUT}")
        import time
        time.sleep(1)

    except Exception:
        import traceback
        print("An unexpected error occurred during script execution.")
        traceback.print_exc()

if __name__ == "__main__":
    update_ifc_model(
        PEDESTAL_WIDTH=0.5,
        PEDESTAL_HEIGHT=0.6,
        SLAB_THICK=0.4,
        SLAB_BASE_SIZE=3
    )