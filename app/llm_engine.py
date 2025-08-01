import os
import urllib.parse
import requests
import viktor as vkt
import logging
import pprint
import instructor
import plotly.graph_objects as go
import app.crud.data_management.helpers as aps_helpers

from pydantic import BaseModel, Field
from dotenv import load_dotenv
from openai import OpenAI
from openai.types.chat import ParsedChatCompletion
from typing import Union
from textwrap import dedent

from app.types import MembersDict, CrossSectionInfo
from app.plots.model_viz import plot_3d_model
from app.plots.piles import plot_3d_with_foundations, collect_support_nodes, group_four_pile_sets
from app.plots.footings import plot_structure_with_footing, group_four_pedestal_sets, collect_support_nodes as collect_footnng_support_nodes
from app.plots.caisson import group_caisson_locations, plot_3d_with_caissons
from app.geometry.utils import get_nodes_lines
from app.opensees.model import Model, calculate_displacements
from app.plots.model_defo import plot_deformed_mesh
logger = logging.getLogger(__name__)
load_dotenv()
client = instructor.from_openai(OpenAI())

class PlotModel(BaseModel):
    pass 

class RunModel(BaseModel):
    why: str = Field(..., description="Use this tool to analyse or run the model")
    pass

class Upload2Acc(BaseModel):
    note: str = Field(..., description="Use this tool when the user want to send the updated model to ACC autodesk platrom services")
    pass
   
class PlotModelWithPiles(BaseModel):
    PILE_DIAM: float = Field(..., description="Pile diameter")
    PILE_LENGTH: float = Field(..., description="Pile length")
    CAP_THICK: float = Field(..., description="Pile Cap thickness")
    EDGE_COVER: float = Field(..., description="distance between the piles and the cap edge")
    CLUSTER_TOL: float = Field(..., description="Cluter Tolerance default 3m")

class PlotFootingModel(BaseModel):
    PEDESTAL_WIDTH: float = Field(..., description="Width of the square pedestals.")
    PEDESTAL_HEIGHT: float = Field(..., description="Height of the pedestals from the slab.")
    SLAB_THICK: float = Field(..., description="Thickness of the footing slab.")
    EDGE_COVER: float = Field(..., description="Distance from outer pedestals to the slab edge.")
    CLUSTER_TOL: float = Field(default=3.0, description="Tolerance for grouping support nodes.")

class PlotModelWithCaisson(BaseModel):
    """Pydantic model defining the parameters for a caisson foundation."""
    CAISSON_WIDTH: float = Field(..., description="Caisson width (along the X-axis)")
    CAISSON_DEPTH: float = Field(..., description="Caisson depth (along the Y-axis)")
    CAISSON_THICKNESS: float = Field(..., description="Caisson thickness or height (along the Z-axis)")
    CLUSTER_TOL: float = Field(..., description="Tolerance for grouping support nodes into a single foundation")



# FOUNDATION_PARAMS = {
#     'PILE_DIAM': 1.0,       # meters
#     'PILE_LENGTH': 10.0,    # meters
#     'CAP_THICK': 1.2,       # meters
#     'EDGE_COVER': 0.4,      # meters
#     'CLUSTER_TOL': 3.0      # meters}

class Response(BaseModel):
    response: str = Field(..., description="Be conversational firendly and Format the response always nicely")
    selected_tool: Union[None , PlotModel, PlotModelWithPiles ,PlotFootingModel, PlotModelWithCaisson, RunModel | Upload2Acc] = Field(..., description="Select any of these tools, Use any of   ")


def llm_response(conversation_history: list[dict],
                 verbose: bool = True) -> ParsedChatCompletion[Response]:
    
    messages = []
    # Default system prompt is always first
    system_message = {
        "role": "system",
        "content": dedent(
            """
            You are a helpful assistant with the following context, who formats responses clearly and helps users analyze structures comming
            from Autodesk Construcction Cloud (ACC) using the VITKOR - APS Integration.

            Use AnalyzeModel to Analyze the model do not confuse that with 
            """
        )
    }
    messages.append(system_message)
    messages.extend(conversation_history)
    if verbose:
        logger.debug("Request messages:\n%s", pprint.pformat(messages))
    
    resp_chunks = client.chat.completions.create_partial(
        model="gpt-4.1",
        messages=messages,
        response_model=Response,
        temperature=0.3,
    )

    resp_final = None
    # Streaming
    for resp in resp_chunks:
        if verbose:
            logger.debug("Received response chunk:\n%s", resp)
        resp_final: Response = resp 

    return resp_final

def get_model():
    raw = vkt.Storage().get("ifc_model", scope="entity").getvalue()
    nodes, lines = get_nodes_lines(file=raw)


    my_cs = CrossSectionInfo(
        name = "L70x4",
        id = 1,
        A=100,
        Iz=10000000000000000000,
        Iy=10000000000000000000,
        Jxx=20000000,
        b=0.070,
        h=0.070
    )
    members: MembersDict = {}
    cs_dict = {1 : my_cs}
    for line_id in lines:
        members[line_id] = {"line_id":line_id, "cross_section_id":1, "material_name": "Steel"}
    return nodes, lines, members, cs_dict

def execute_tool(response: Response) -> tuple[str, go.Figure | None]:
    """Exectue the tools based on the user query and file_content. Generates a text response
    or a Plotly view."""
    print(f"[Debug] {response}")
    if isinstance(response.selected_tool, PlotModel):
        nodes, lines, members, cs_dict = get_model()
        fig = plot_3d_model(nodes, lines, members, cs_dict)
        # print(fig)
        return response.response, fig
    
    if isinstance(response.selected_tool, PlotModelWithPiles):
        nodes, lines, members, cs_dict = get_model()
        foundation_params_dict = response.selected_tool.model_dump()
        support_nodes = collect_support_nodes(nodes)
        caps = group_four_pile_sets(support_nodes, cluster_tol=foundation_params_dict['CLUSTER_TOL'])

        fig = plot_3d_with_foundations(nodes=nodes, lines=lines, members=members,cross_sections=cs_dict, caps=caps, foundation_params=foundation_params_dict)
        return response.response, fig
            
    if isinstance(response.selected_tool, PlotFootingModel):
        nodes, lines, members, cs_dict = get_model()
        footing_params_dict = response.selected_tool.model_dump()
        support_nodes = collect_footnng_support_nodes(nodes)
        footings = group_four_pedestal_sets(support_nodes, cluster_tol=footing_params_dict['CLUSTER_TOL'])

        fig = plot_structure_with_footing(nodes=nodes, lines=lines, members=members, cross_sections=cs_dict, footings=footings, footing_params=footing_params_dict)
        return response.response, fig

    if isinstance(response.selected_tool, PlotModelWithCaisson):
        nodes, lines, members, cs_dict = get_model()
        footing_params_dict = response.selected_tool.model_dump()
        support_nodes = collect_support_nodes(nodes)
        caisson_locations = group_caisson_locations(
            support_nodes,
            cluster_tol=footing_params_dict['CLUSTER_TOL']
        )
        fig = plot_3d_with_caissons(
            nodes=nodes,
            lines=lines,
            members=members,
            cross_sections=cs_dict,
            caissons=caisson_locations,
            foundation_params=footing_params_dict
        )
        return response.response, fig
    
    if isinstance(response.selected_tool, RunModel):
        nodes, lines, members, cs_dict = get_model()
        my_model = Model(
            nodes=nodes, lines=lines, cross_sections=cs_dict, members=members,
        )
        my_model.create_model()
        my_model.run_model()
        
        disp_dict = calculate_displacements(lines=lines, nodes=nodes)
        
        fig = plot_deformed_mesh(disp_dict=disp_dict, members=members, cross_sections= cs_dict, nodes=nodes, lines=lines)
        return response.response, fig
    
    if isinstance(response.selected_tool, Upload2Acc):

        """Upload an IFC file to ACC in the folder selected in the UI."""
        # Get token and parameters from UI
        integration = vkt.external.OAuth2Integration("aps-integration-1")
        token = integration.get_access_token()
        # Get project and folder IDs
        hub_id = aps_helpers.get_hub_id_by_name(token, "alejandroduartevendries@gmail.com")
        print("hub",hub_id)
        project_id = "b.729915f2-f25c-45da-b435-b08606ef395b"#aps_helpers.get_project_id_by_name(token, hub_id, "Sample Project - Seaport Civic Center")
        print("project id",project_id)
        # Get folder ID for the selected subfolder path
        all_paths_with_ids = aps_helpers.get_all_folder_paths_with_ids(hub_id, project_id, token)
        folder_id = "urn:adsk.wipprod:fs.folder:co.znBylNjGSiOypYNeAgrIbw"#next((fid for path, fid in all_paths_with_ids if path =="Files/Structural"), None) # Files/Structural
        
        if not folder_id:
            print("Could not find folder ID for path 'Files/Structural'")
            return
        
        # Read the IFC file from disk
        file_name = "Substation_Gantry_GA_with_piles.ifc"
        ifc_file_path = os.path.join(os.path.dirname(__file__), 'geometry', file_name)
        
        with open(ifc_file_path, 'rb') as file:
            file_content_bytes = file.read()
        
        # Constants
        APS_BASE_URL = "https://developer.api.autodesk.com"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/vnd.api+json"}

        # === Step 1: Create a Storage Location ===
        storage_create_url = f"{APS_BASE_URL}/data/v1/projects/{project_id}/storage"
        storage_payload = {
            "jsonapi": {"version": "1.0"},
            "data": {
                "type": "objects",
                "attributes": {
                    "name": file_name
                },
                "relationships": {
                    "target": {
                        "data": {
                            "type": "folders",
                            "id": folder_id
                        }
                    }
                }
            }
        }
        
        print("Step 1: Creating storage location...")
        r_response = requests.post(storage_create_url, headers=headers, json=storage_payload)
        r_response.raise_for_status()
        storage_urn = r_response.json()["data"]["id"]
        print(f"  > Storage URN created: {storage_urn}")

        # The storage URN contains the bucket key and object key needed for the next steps
        urn_parts = storage_urn.split(':')
        object_id = urn_parts[-1]
        bucket_key, object_key = object_id.split('/')
        encoded_bucket_key = urllib.parse.quote(bucket_key)
        encoded_object_key = urllib.parse.quote(object_key)

        # === Step 2 (continued): Get Signed S3 Upload URL & Upload the File ===
        signed_upload_url = f"{APS_BASE_URL}/oss/v2/buckets/{encoded_bucket_key}/objects/{encoded_object_key}/signeds3upload"
        print("Step 2: Getting S3 signed URL and uploading file...")
        s3_response = requests.get(signed_upload_url, headers={"Authorization": f"Bearer {token}"})
        s3_response.raise_for_status()
        s3_data = s3_response.json()

        # You MUST capture the uploadKey from the response
        upload_key = s3_data['uploadKey']
        upload_url = s3_data['urls'][0]

        # Upload the file content to the S3 URL
        upload_response = requests.put(upload_url, data=file_content_bytes, headers={"Content-Type": "application/octet-stream"})
        upload_response.raise_for_status()
        print(f"  > File '{file_name}' uploaded to S3.")


        # === THIS IS STEP 8 FROM THE TUTORIAL - THE MISSING PIECE ===
        print("Step 2.5 (Completing Upload): Finalizing the upload with APS...")
        complete_headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        complete_payload = {
            "uploadKey": upload_key
        }

        # The endpoint is the *same* one used to get the signed URL
        complete_response = requests.post(signed_upload_url, headers=complete_headers, json=complete_payload)
        complete_response.raise_for_status()
        print("  > Upload finalized and storage URN is now valid.")


        # === THIS IS STEP 9 FROM THE TUTORIAL - CREATING THE ITEM ===
        # Your code for this step is now correct and should work.
        item_create_url = f"{APS_BASE_URL}/data/v1/projects/{project_id}/items"
        item_payload = {
            "jsonapi": {"version": "1.0"},
            "data": {
                "type": "items",
                "attributes": {
                    "displayName": file_name,
                    "extension": {"type": "items:autodesk.bim360:File", "version": "1.0"}
                },
                "relationships": {
                    "tip": {"data": {"type": "versions", "id": "1"}},
                    "parent": {"data": {"type": "folders", "id": folder_id}}
                }
            },
            "included": [{
                "type": "versions",
                "id": "1",
                "attributes": {
                    "name": file_name,
                    "extension": {"type": "versions:autodesk.bim360:File", "version": "1.0"}
                },
                "relationships": {"storage": {"data": {"type": "objects", "id": storage_urn}}}
            }]
        }

        print("Step 3 (Creating Item): Creating the file item in ACC...")
        final_response = requests.post(item_create_url, headers=headers, json=item_payload)
        final_response.raise_for_status()

        # If you reach here, it was successful.
        print(f"  > SUCCESS! File '{file_name}' is now visible in ACC.")
        return response.response, None
    
    return response.response, None