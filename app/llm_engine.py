import os
import io
import urllib.parse
import requests
import viktor as vkt
import logging
import pprint
import instructor
import plotly.graph_objects as go
import app.crud.data_management.helpers as aps_helpers

from app.agent.router import agent_router
from app.agent.structural_agent import structural_agent, StructuralTools
from app.agent.back_call import llm_back_call
from app.agent.geothechnical_input_agent import geothecnical_input_agent, execute_inputs_for_foundation
from app.tools import PlotOpenSeesModelTool, DisplayLoadsTool, RunOpenSeesModelTool,GetGeotechnicalInputsForFoundationDesign, PullGeotechnicalReportTool, FootingDesignTool, UploadToAccTool
from app.agent.geothecnical_design_agent import geotechnical_design_agent

from pydantic import BaseModel, Field
from dotenv import load_dotenv
from openai import OpenAI
from openai.types.chat import ParsedChatCompletion
from typing import Union
from textwrap import dedent
import pdfminer.high_level

from app.types import MembersDict, CrossSectionInfo, NodesDict
from app.plots.model_viz import plot_3d_model
from app.plots.piles import plot_3d_with_foundations, collect_support_nodes, group_four_pile_sets
from app.foundations.footings.plots_footings import plot_structure_with_footing, group_four_pedestal_sets, collect_support_nodes as collect_footnng_support_nodes
from app.plots.caisson import group_caisson_locations, plot_3d_with_caissons
from app.geometry.utils import get_nodes_lines
from app.opensees.model import Model, calculate_displacements, calculate_reactions
from app.plots.model_defo import plot_deformed_mesh

from app.foundations.footings.footings import store_footing_iterations_as_table, FootingSoilData
from app.foundations.piles.piles import  PilesDesignTools, store_pile_iterations_as_table

from app.geometry.utils import read_nodal_loads, calculate_center_loads_foundation
from app.plots.model_with_loads import plot_3d_model_with_loads

logger = logging.getLogger(__name__)
load_dotenv()
client = instructor.from_openai(OpenAI())






def convert_model_to_mm(nodes: NodesDict) -> NodesDict:
    nodes_in_mm: NodesDict = {}
    # Convert model to mm for opensees
    for nodes_id, node_vals in nodes.items():
        nodes_in_mm[nodes_id] = {"id": nodes_id, "x": node_vals["x"]*1000, "y": node_vals["y"]*1000, "z": node_vals["z"]*1000}
    return nodes_in_mm
    

def convert_cs_to_m(cs_dict: dict[int, CrossSectionInfo])-> dict[int, CrossSectionInfo]:
    cs_dict_m: dict[int, CrossSectionInfo] = {}
    # Convert cross sections to m
    for cs_dict_key, info in cs_dict.items():
        info["b"] = info["b"]/1000
        info["h"] = info["h"]/1000
        cs_dict_m[cs_dict_key] = info
    return cs_dict_m

# class Response(BaseModel):
#     response: str = Field(..., description="Be conversational, friendly and format the response always nicely")
#     tool: Union[
#         None,
#         PilesDesignTools,
#         PlotOpenSeesModelTool,
#         RunOpenSeesModelTool,
#         UploadToAccTool,
#         FootingDesignTool,
#         DisplayLoadsTool,
#         PullGeotechnicalReportTool,
#         GetGeotechnicalInputsForFoundationDesign
#     ] = Field(..., description="""Select any of these tools.""")

def llm_response(conversation_history: list[dict],
                 verbose: bool = True) -> ParsedChatCompletion[StructuralTools] | None:
    
    route = agent_router(conversation_history=conversation_history)
    
    # request_type: Literal["Structural", "GeotechnicalInput", "GeotechnicalDesign", "None"]
    if route.request_type == "Structural":
        #     tool: Union[PlotOpenSeesModelTool, RunOpenSeesModelTool, DisplayLoadsTool]
        tool_choice = structural_agent(conversation_history=conversation_history)
        return tool_choice

    if route.request_type == "GeotechnicalInput":
        tool_choice = geothecnical_input_agent(conversation_history=conversation_history)
        return tool_choice

    if route.request_type == "GeotechnicalDesign":
        tool_choice = geotechnical_design_agent(conversation_history=conversation_history)
        return tool_choice

    # if verbose:
    #     logger.debug("Request messages:\n%s", pprint.pformat(messages))
    
    # resp_chunks = client.chat.completions.create_partial(
    #     model="gpt-4.1-mini",
    #     messages=messages,
    #     response_model=Response

    # )
    # resp_final = None
    # # Streaming
    # for resp in resp_chunks:
    #     if verbose:
    #         logger.debug("Received response chunk:\n%s", resp)
    #     resp_final: Response = resp 

    # return resp_final

def get_model():
    raw = vkt.Storage().get("ifc_model", scope="entity").getvalue()
    nodes, lines = get_nodes_lines(file=raw)

    my_cs = CrossSectionInfo(
        name="L3-1/2x3-1/2x1/4",
        id=1,
        A=1096.8,           # mm^2
        Iz=1308881.8,       # mm^4
        Iy=340048.0,        # mm^4
        Jxx=16066.5,        # mm^4 (torsion constant)
        b=88.9,             # mm width
        h=88.9              # mm depth
    )
    members: MembersDict = {}
    cs_dict = {1 : my_cs}
    for line_id in lines:
        members[line_id] = {"line_id":line_id, "cross_section_id":1, "material_name": "Steel"}
    return nodes, lines, members, cs_dict

def execute_tool(response: StructuralTools, conversation: list[dict] | None = None) -> tuple[str, go.Figure | None]:
    """Exectue the tools based on the user query and file_content. Generates a text response
    or a Plotly view."""
    print(f"[DEGUN], {response=}")
    # Fix the isinstance check - make sure the class is correctly identified
    if isinstance(response.tool, PlotOpenSeesModelTool):
        nodes, lines, members, cs_dict = get_model()
        cs_dict_m = convert_cs_to_m(cs_dict=cs_dict)
        fig = plot_3d_model(nodes, lines, members, cs_dict_m)
        return response.response, fig
    
    if isinstance(response.tool, DisplayLoadsTool):
        nodes, lines, members, cs_dict = get_model()
        cs_dict_m = convert_cs_to_m(cs_dict=cs_dict)
        
        raw = vkt.Storage().get("ifc_model", scope="entity").getvalue()
        loads_dict = read_nodal_loads(raw)
        fig = plot_3d_model_with_loads(nodes,lines,members, cs_dict, loads_dict)
        if conversation:
            tool_result = f" The tool generated the following results: load node IDs and magnitudes [kN] {loads_dict}. Tell the user wind loads are applied on the structure but they are not shown. The loads shown in the scene belong to the critical load for foundation design (wire loads + wind). Loads will be rendered in the RHS of the view. List the loads in bullet points for the user but do not make a markdown table."
            # new_response = llm_response(conversation_history=conversation)
            back_response = llm_back_call(assitant_message=response.response, tool_result=tool_result, conversation_history=conversation)
            if back_response:
                return back_response, fig
        # fig = plot_3d_model(nodes, lines, members, cs_dict_m)
        # print(fig)
        return response.response, fig

    if isinstance(response.tool, RunOpenSeesModelTool):
        nodes, lines, members, cs_dict = get_model()
        raw = vkt.Storage().get("ifc_model", scope="entity").getvalue()
        loads_dict = read_nodal_loads(raw)
        nodes_m = convert_model_to_mm(nodes)
        my_model = Model(
            nodes=nodes_m, lines=lines, cross_sections=cs_dict, members=members, nodal_loads=loads_dict
        )
        my_model.create_model()
        my_model.run_model()
        reactions = calculate_reactions(nodes=nodes)
        center_loads = calculate_center_loads_foundation(reactions=reactions, nodes=nodes)
        disp_dict = calculate_displacements(lines=lines, nodes=nodes)
        # Convert displacements back from mm to m! (models are in m)
        disp_dict_m: dict[int, dict[str, float]] = {}
        for node_id, defo in disp_dict.items():
            disp_dict_m[node_id] = {"x": defo["x"]/1000, "y": defo["y"]/1000, "z": defo["z"]/1000} 
        cs_dict_m = convert_cs_to_m(cs_dict=cs_dict)
        fig = plot_deformed_mesh(disp_dict=disp_dict_m, members=members, cross_sections= cs_dict_m, nodes=nodes, lines=lines)
    
        tool_result = f" The tool generated the following results: Reaction loads [N] {reactions} Newtons, design load at the center of each foundation (kN and kN·m): {center_loads}, deformed shape of the model will be displayed in the RHS of the app."
        new_response = llm_back_call(conversation_history=conversation, assitant_message=response.response, tool_result=tool_result)

        if new_response:
            return new_response, fig
        return response.response, fig

    if isinstance(response.tool, PullGeotechnicalReportTool):
        # integration = vkt.external.OAuth2Integration("aps-integration-1")
        # token = integration.get_access_token()
        # file_content = aps_helpers.get_file_content(
        #     token=token,
        #     hub_name= "alejandroduartevendries@gmail.com",
        #     project_name="Construction : Sample Project - Seaport Civic Center",
        #     subfolder_path="Project Files/Structural/Geotechnical",
        #     file_name="GEO001 - GEOTECHNICAL DATA SUMMARY REV0.pdf",
        # )
        # # print(f"{file_content=}, {type(file_content)=}")
       
        # vkt.Storage().set(
        #     "geotechnical_report",
        #     data=vkt.File.from_data(file_content),
        #     scope="entity",
        # )
        raw: bytes = vkt.Storage().get("geotechnical_report", scope="entity").getvalue_binary()
        pdf_stream = io.BytesIO(raw)
        text = pdfminer.high_level.extract_text(pdf_stream)
        print(f"{text=}")
        tool_result = f" Based on the text, get the required soil parameters to design the foundation: {text}. Tell the user the inputs to be used and if they want to proceed to DESIGN the foundation with these parameters. Do not make markdown tables in chat!"
        new_response = llm_back_call(assitant_message=response.response, tool_result=tool_result,conversation_history=conversation)
        return new_response, None
    
    if isinstance(response.tool, GetGeotechnicalInputsForFoundationDesign):
        raw: bytes = vkt.Storage().get("geotechnical_report", scope="entity").getvalue_binary()
        pdf_stream = io.BytesIO(raw)
        text = pdfminer.high_level.extract_text(pdf_stream)
        geotechnicalInputTools = execute_inputs_for_foundation(input=response.tool,context=text)
        return geotechnicalInputTools.response, None

    if isinstance(response.tool, PilesDesignTools):
        nodes, lines, members, cs_dict = get_model()
        raw = vkt.Storage().get("ifc_model", scope="entity").getvalue()
        loads_dict = read_nodal_loads(raw)
        nodes_m = convert_model_to_mm(nodes)
        my_model = Model(
            nodes=nodes_m, lines=lines, cross_sections=cs_dict, members=members,nodal_loads=loads_dict
        )
        my_model.create_model()
        my_model.run_model()
        reactions = calculate_reactions(nodes=nodes)

        
        from app.foundations.piles.piles import find_optimal_pile
        best, iterations = find_optimal_pile(response.tool.soil)
        store_pile_iterations_as_table(pile_iterations=iterations)
        pile_geometry, cost, h_strenght, compression_strenght, tension_strenght = best
        if not pile_geometry:
            raise ValueError("Optimization Fail")
        pile_params_dict = pile_geometry.model_dump()
        support_nodes = collect_support_nodes(nodes)
        caps = group_four_pile_sets(support_nodes, cluster_tol=pile_params_dict['CLUSTER_TOL'])
        cs_dict_m = convert_cs_to_m(cs_dict=cs_dict)

        fig = plot_3d_with_foundations(nodes=nodes, lines=lines, members=members,cross_sections=cs_dict_m, caps=caps, foundation_params=pile_params_dict)

        if conversation:
            conversation.append({"role":"assistant","content":response.response})
            tool_result = f" The tool generated the following results: Optimal geometry {pile_geometry}, construction cost: ${cost} USD, lateral strength: {h_strenght} kN, compression strength {compression_strenght} kPa and tension strength {tension_strenght} kPa with a safety factor of 2 and 2.5 respectively. Let the user know the foundation model will be rendered in the RHS view."

            new_response = llm_back_call(conversation_history=conversation,assitant_message=response.response, tool_result=tool_result)
            
            if new_response:
                return new_response, fig

    if isinstance(response.tool, FootingDesignTool):

        # soil = response.tool.footing_soil_data
        # for entry in response.tool.soil.bearing_table:
        #     if not entry.qadm:
        #         if conversation:
        #             raw: bytes = vkt.Storage().get("geotechnical_report", scope="entity").getvalue_binary()
        #             pdf_stream = io.BytesIO(raw)
        #             text = pdfminer.high_level.extract_text(pdf_stream)
        #             conversation.append({"role":"assistant","content":str(response.tool)})
        #             conversation.append({"role":"user", "content": f"in your last answer one or more values of qadm were None give the correct bearing table input using the data {text}"})
        #             new_response = llm_response(conversation_history=conversation)
        #             if isinstance(new_response.tool, FootingDesignTool):
        #                 soil = new_response.tool.footing_soil_data
        #                 break
                    
        
        nodes, lines, members, cs_dict = get_model()

        nodes_m = convert_model_to_mm(nodes)
        raw = vkt.Storage().get("ifc_model", scope="entity").getvalue()
        loads_dict = read_nodal_loads(raw)
        my_model = Model(
            nodes=nodes_m, lines=lines, cross_sections=cs_dict, members=members, nodal_loads=loads_dict
        )
        my_model.create_model()
        my_model.run_model()
        reactions = calculate_reactions(nodes=nodes)
        center_loads = calculate_center_loads_foundation(reactions=reactions, nodes=nodes)
        print(center_loads)
        from app.foundations.footings.footings import find_optimal_footing_geometry
        footing_geometry, cost, soil_pressure, iterations = find_optimal_footing_geometry(response.tool.footing_soil_data, center_loads)
        store_footing_iterations_as_table(footing_iterations=iterations)
        if not footing_geometry:
            raise ValueError("Optimization Fail")
        footing_params_dict = footing_geometry.model_dump()
        support_nodes = collect_footnng_support_nodes(nodes)
        footings = group_four_pedestal_sets(support_nodes, cluster_tol=footing_params_dict['CLUSTER_TOL'])
        cs_dict_m = convert_cs_to_m(cs_dict=cs_dict)

        fig = plot_structure_with_footing(nodes=nodes, lines=lines, members=members, cross_sections=cs_dict_m, footings=footings, footing_params=footing_params_dict)

        if conversation:
            conversation.append({"role":"assistant","content":response.response})
            conversation.append({"role":"user", "content": f" The tool generated the following results: Optimal geometry {footing_geometry}, construction cost: USD {cost}, acting soil pressure: {soil_pressure}. Let the user know the foundation model will be rendered in the RHS view."})
            new_response = llm_response(conversation_history=conversation)
            if new_response:
                return new_response.response, fig
            
    if isinstance(response.tool, UploadToAccTool):

        """Upload an IFC file to ACC in the folder selected in the UI."""

        # Updat IFC Model
        from app.foundations.footings.ifc_footing import update_ifc_model
        
        update_ifc_model(PEDESTAL_WIDTH = response.tool.PEDESTAL_WIDTH, PEDESTAL_HEIGHT=response.tool.PEDESTAL_WIDTH, SLAB_THICK=response.tool.SLAB_THICK, SLAB_BASE_SIZE = response.tool.SLAB_BASE_SIZE)
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
        folder_id = "urn:adsk.wipprod:fs.folder:co.1NV3xBdHRmqbWaD_s2pR4Q"#next((fid for path, fid in all_paths_with_ids if path =="Files/Structural"), None) # Files/Structural
        
        if not folder_id:
            print("Could not find folder ID for path 'Files/Structural'")
            return
        
        # Read the IFC file from disk
        file_name = "Substation_Gantry_GA_with_footings.ifc"
        # file_name = "Substation_Gantry_GA_with_piles.ifc"

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



    

        
    #     return response.response, fig
    
   
    


    # if isinstance(response.tool, PullGeotechnicalReportTool):    
    #     integration = vkt.external.OAuth2Integration("aps-integration-1")
    #     token = integration.get_access_token()

    #     # file_content = aps_helpers.get_file_content(
    #     #     token=token,
    #     #     hub_name=params.step1.hubs,
    #     #     project_name=params.step1.project,
    #     #     subfolder_path=params.step1.subfolder_path,
    #     #     file_name=params.step1.files,
    #     # )

        # file_content = aps_helpers.get_file_content(
        #     token=token,
        #     hub_name= "alejandroduartevendries@gmail.com",
        #     project_name="Construction : Sample Project - Seaport Civic Center",
        #     subfolder_path="Project Files/Structural/Geotechnical",
        #     file_name="GEO001 - GEOTECHNICAL DATA SUMMARY REV0.pdf",
        # )
        # # print(f"{file_content=}, {type(file_content)=}")
       
        # vkt.Storage().set(
        #     "geotechnical_report",
        #     data=vkt.File.from_data(file_content),
        #     scope="entity",
        # )
    #     raw: bytes = vkt.Storage().get("geotechnical_report", scope="entity").getvalue_binary()
    #     pdf_stream = io.BytesIO(raw)
    #     text = pdfminer.high_level.extract_text(pdf_stream)

    #     if conversation:
    #         conversation.append({"role":"assistant","content":response.response})
    #         conversation.append({"role":"user", "content": f" The tool retrieved the following report from the folder: Files/Structural/Geotechnical: {text}. Tell the user a succinct summary of the content, and if they want to design the foundation of the structural model using this data. You have the data to design piles, footings, and monopiles!"})
    #         new_response = llm_response(conversation_history=conversation)
    #         if new_response:
    #             return new_response.response, None
    #     return new_response.response, None


    # return response.response, None