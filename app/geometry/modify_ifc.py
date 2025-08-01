"""
add_pile_caps_centered.py
--------------------------------------------------
This script adds pile caps and piles to the GA model,
using lines and nodes extracted from the structural model.
"""

import ifcopenshell
from pathlib import Path
from collections import defaultdict
import logging
from math import sqrt


STRUCT_IFC = Path(r"models\Substation_Gantry_Structural.ifc")
GA_IFC_IN  = Path(r"models\Substation_Gantry_GA.ifc")
GA_IFC_OUT = Path(r"models\Substation_Gantry_GA_with_piles.ifc")

PILE_DIAM   = 1.0    # m
PILE_LENGTH = 10.0   # m  (extrude downwards)
CAP_THICK   = 1.2    # m
EDGE_COVER  = 0.4    # m  (clear distance beyond extreme piles)
CLUSTER_TOL = 3.0    # m


def get_nodes_lines():
    logging.info(f"Attempting to open structural model: {STRUCT_IFC}")
    model = ifcopenshell.open(STRUCT_IFC.as_posix())
    logging.info("Successfully opened structural model.")

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
    logging.info(f"Extracted {len(nodes)} structural nodes.")

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
    logging.info(f"Extracted {len(lines)} structural lines (members).")
    return nodes, lines


def collect_support_nodes(nodes, z_tol=1e-6):
    support_nodes = {n['id']: n for n in nodes.values() if abs(n['z']) < z_tol}
    logging.info(f"Found {len(support_nodes)} support nodes at Z=0 (tolerance={z_tol}).")
    return support_nodes

def group_four_pile_sets(supports, cluster_tol=CLUSTER_TOL):
    logging.info(f"Grouping supports into clusters with a tolerance of {cluster_tol}m.")
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
            logging.info(f"Found a valid 4-pile cluster (Cap #{cap_id}) with nodes: {[n['id'] for n in current_cluster]}")
            caps[cap_id] = current_cluster
            cap_id += 1
        else:
            logging.warning(f"Discarding a cluster with {len(current_cluster)} nodes: {[n['id'] for n in current_cluster]}")
    if not caps: logging.warning("Could not find any valid 4-pile groups.")
    else: logging.info(f"Successfully identified {len(caps)} pile cap groups.")
    return caps

def centroid(nodes):
    node_coords = [(n['x'], n['y']) for n in nodes]
    logging.debug(f"Calculating centroid from node coordinates: {node_coords}")
    cx = sum(n['x'] for n in nodes) / 4.0
    cy = sum(n['y'] for n in nodes) / 4.0
    logging.info(f"Calculated middle point (centroid) between legs at: (X={cx:.3f}, Y={cy:.3f})")
    return cx, cy

# ------------------------------------------------------------------
# 3. IFC helpers
# ------------------------------------------------------------------
def rect_profile(mdl, w, d):
    p2d = mdl.create_entity("IfcAxis2Placement2D", mdl.create_entity("IfcCartesianPoint", (0.0, 0.0)), None)
    return mdl.create_entity("IfcRectangleProfileDef", "AREA", None, p2d, w, d)

def circle_profile(mdl, r):
    p2d = mdl.create_entity("IfcAxis2Placement2D", mdl.create_entity("IfcCartesianPoint", (0.0, 0.0)), None)
    return mdl.create_entity("IfcCircleProfileDef", "AREA", None, p2d, r)

def extrude(mdl, profile, origin, direction, depth):
    place3d = mdl.create_entity("IfcAxis2Placement3D", origin, None, None)
    return mdl.create_entity("IfcExtrudedAreaSolid", profile, place3d, direction, depth)

def body_rep(mdl, solid):
    ctx = mdl.by_type("IfcGeometricRepresentationContext")[0]
    return mdl.create_entity("IfcShapeRepresentation", ctx, "Body", "SweptSolid", [solid])

def prod_shape(mdl, rep):
    return mdl.create_entity("IfcProductDefinitionShape", None, None, [rep])

# ------------------------------------------------------------------
# 4. Main Routine
# ------------------------------------------------------------------
def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Starting script: add_pile_caps_centered.py")
    
    try:
        logging.info("--- Phase 1: Data Extraction and Grouping ---")
        nodes, _ = get_nodes_lines()
        supports = collect_support_nodes(nodes)
        caps = group_four_pile_sets(supports)

        if not caps:
            logging.warning("No pile cap groups were found. Exiting script.")
            return

        logging.info("--- Phase 2: Creating IFC Geometries ---")
        logging.info(f"Opening General Arrangement model: {GA_IFC_IN}")
        ga_model = ifcopenshell.open(GA_IFC_IN.as_posix())
        
        oh, site = ga_model.by_type("IfcOwnerHistory")[0], ga_model.by_type("IfcSite")[0]
        new_guid, neg_z = ifcopenshell.guid.new, ga_model.create_entity("IfcDirection", (0.0, 0.0, -1.0))
        created = []
        
        # --- FIX IS HERE ---
        # Create one single point at (0,0,0) to be reused for all local shape definitions
        o_local = ga_model.create_entity("IfcCartesianPoint", (0.0, 0.0, 0.0))
        
        for cap_id, pile_nodes in caps.items():
            logging.info(f"--- Processing Pile Cap Group #{cap_id} ---")
            cx, cy = centroid(pile_nodes)
            
            xs, ys = [n['x'] for n in pile_nodes], [n['y'] for n in pile_nodes]
            width, depth = (max(xs) - min(xs)) + 2*EDGE_COVER, (max(ys) - min(ys)) + 2*EDGE_COVER
            z_top = 0.0

            # ----- footing (Pile Cap) -----
            logging.info(f"Placing IfcFooting (Pile Cap #{cap_id}) at global coordinate (X={cx:.3f}, Y={cy:.3f}, Z={z_top:.3f})")
            
            # 1. Define the PRODUCT's placement at the global centroid coordinate
            o_cap_global = ga_model.create_entity("IfcCartesianPoint", (cx, cy, z_top))
            place_cap_global = ga_model.create_entity("IfcLocalPlacement", None, ga_model.create_entity("IfcAxis2Placement3D", o_cap_global, None, None))
            
            # 2. Define the SHAPE's geometry at the local origin (0,0,0)
            rect_prof = rect_profile(ga_model, width, depth)
            solid_cap = extrude(ga_model, rect_prof, o_local, neg_z, CAP_THICK) # Use o_local here
            shape_cap = prod_shape(ga_model, body_rep(ga_model, solid_cap))

            # 3. Create the footing, assigning the global placement and the local shape
            footing = ga_model.create_entity("IfcFooting", new_guid(), oh, f"PileCap_{cap_id}", None, None, place_cap_global, shape_cap, "FOOTING")
            created.append(footing)

            # ----- piles -----
            # The logic for piles was already correct, as each is its own product
            logging.debug(f"Creating {len(pile_nodes)} piles for Cap #{cap_id}...")
            circ_prof = circle_profile(ga_model, PILE_DIAM / 2)
            for i, n in enumerate(pile_nodes, start=1):
                # Each pile gets its own global placement
                o_p_global = ga_model.create_entity("IfcCartesianPoint", (n['x'], n['y'], z_top))
                place_p_global = ga_model.create_entity("IfcLocalPlacement", None, ga_model.create_entity("IfcAxis2Placement3D", o_p_global, None, None))
                
                # The shape is defined locally at (0,0,0)
                solid_p = extrude(ga_model, circ_prof, o_local, neg_z, PILE_LENGTH) # Use o_local here too
                shape_p = prod_shape(ga_model, body_rep(ga_model, solid_p))
                
                pile = ga_model.create_entity("IfcPile", new_guid(), oh, f"Pile_{cap_id}_{i}", None, None, place_p_global, shape_p, "BORED")
                created.append(pile)

        logging.info(f"--- Finalizing: Total new IFC entities created: {len(created)} ---")
        ga_model.create_entity("IfcRelContainedInSpatialStructure", new_guid(), oh, None, None, created, site)
        ga_model.write(GA_IFC_OUT.as_posix())
        logging.info(f"SUCCESS: Pile caps and piles written to {GA_IFC_OUT}")

    except Exception:
        logging.error("An unexpected error occurred during script execution.", exc_info=True)

if __name__ == "__main__":
    main()