import requests
import base64
import urllib.parse
import viktor as vkt  # type: ignore
import app.crud.data_management as aps_helpers  
from app.views.apsView import APSView, APSresult

import os
import json
import viktor as vkt
import plotly.graph_objects as go

from openai import OpenAI
from dotenv import load_dotenv
from app.plots.model_viz import default_blank_scene
from typing import Literal
from textwrap import dedent
from app.llm_engine import llm_response, execute_tool

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def store_scene(figure: go.Figure, view_name: Literal["view"] = "view") -> None:
    """This function stores the output of a tool call in
    the vkt.Storage object. The storage object can be used to communicate
    between views."""
    vkt.Storage().set(
        view_name,
        data=vkt.File.from_data(figure.to_json().encode()),
        scope="entity",
    )

def get_visibility(params, **kwargs):
    if not params.step3.chat:
        entities = vkt.Storage().list(scope="entity")
        for entity in entities:
            if entity == "optimization_table":
                vkt.Storage().delete("optimization_table", scope="entity")

    try:
        vkt.Storage().get("optimization_table", scope="entity").getvalue()
        return True
    except Exception:
        # If there is no data, then view is hiden.
        return False
    
def get_aps_token():
    integration = vkt.external.OAuth2Integration("aps-integration-1")
    return integration.get_access_token()

@vkt.memoize
def get_hub_list(params, **kwargs):
    token = get_aps_token()
    hub_names = aps_helpers.get_hub_names(token)
    return hub_names if hub_names else ["No hubs found"]
@vkt.memoize
def get_projects(params, **kwargs) -> list[str]:
    if params.step1.hubs:
        token = get_aps_token()
        hub_name = params.step1.hubs
        projects = aps_helpers.get_projects_names_by_hub(token, hub_name)
        return projects
    return ["Select Hub First"]

@vkt.memoize
def get_top_folders_options(params, **kwargs) -> list[str]:
    if params.step1.hubs and params.step1.project:
        token = get_aps_token()
        hub_name = params.step1.hubs
        project_name = params.step1.project
        return aps_helpers.get_hub_top_folders_names(token, hub_name, project_name)
    return ["Select Project First"]

def get_subfolder_paths_options(params, **kwargs) -> list[str]:
    if params.step1.hubs and params.step1.project and params.step1.top_folder:
        token = get_aps_token()
        hub_name = params.step1.hubs
        project_name = params.step1.project
        top_folder = params.step1.top_folder
        return aps_helpers.get_subfolders_paths_from_top_folder(token, hub_name, project_name, top_folder)
    return ["Select Top Folder First"]

def get_folder_files_options(params, **kwargs) -> list[str]:
    if params.step1.hubs and params.step1.project and params.step1.subfolder_path:
        token = get_aps_token()
        hub_name = params.step1.hubs
        project_name = params.step1.project
        subfolder_path = params.step1.subfolder_path
        return aps_helpers.get_files_names_from_sub_folder(token, hub_name, project_name, subfolder_path)
    return ["Select Subfolder First"]

class Parametrization(vkt.Parametrization):
    step1 = vkt.Step("APS Integration", views="show_cad_model")
    step1.title = vkt.Text(dedent(
        """# APS Integration demo

This **VIKTOR tool** allows you to fetch CAD files from **Autodesk Construction Cloud** (ACC) for structural analysis.
An **AI Agent** will assist you in performing structural analysis in OpenSees, designing and optimizing foundations in Python, and updating the CAD file.
You can then send the updated CAD file back to ACC!
"""
    ))
    step1.hubs = vkt.OptionField("Avaliable Hubs", options=get_hub_list)
    step1.project = vkt.OptionField("Avaliable Projects", options=get_projects)
    step1.br2 = vkt.LineBreak()
    step1.top_folder = vkt.OptionField("Top Folders", options=get_top_folders_options)
    step1.subfolder_path = vkt.OptionField("Subfolders Path", options=get_subfolder_paths_options)
    step1.br4 = vkt.LineBreak()
    step1.files = vkt.OptionField("Folder Files", options=get_folder_files_options)
    step1.stored_file = vkt.OptionField("Structural IFC", options=get_folder_files_options)
    step1.br5 = vkt.LineBreak()
    step1.text2 = vkt.Text("## Fetch Data")
    step1.agent_contenxt = vkt.ActionButton("Get files from  ACC", method="store_file_in_app")
    # step1.upload_file = vkt.ActionButton("Upload File to ACC", method="send_data2acc")
    step2 = vkt.Step("Project Location and Wind Parameters", views=[ "show_map", "calculate_wind_pressures"])
    step2.location_title = vkt.Text(" # Project Location")
    step2.location_description = vkt.Text("Select the project location on the map. The application will retrieve the elevation and regional wind speed based on the coordinates.")

    step2.location = vkt.GeoPointField(' ', default=vkt.GeoPoint(36.16580333278468, -86.78240505816933))
    step2.wind_parameters = vkt.Text("# Wind Design Parameters")
    step2.wind_descrition = vkt.Text("Define the structural parameters for wind load calculation. Select the exposure and risk categories according to the site conditions. Enter the overall structure height and the solidity ratio, which represents the projected solid area divided by the gross face area.")
    # Input fields as specified
    step2.exposure_category = vkt.OptionField(
        'Exposure category', 
        options=['B', 'C', 'D'], 
        default='B'
    )
    
    step2.risk_category = vkt.OptionField(
        'Risk category', 
        options=['I', 'II', 'III', 'IV'], 
        default='III'
    )
    step2.lbrk = vkt.LineBreak()
    step2.overall_height = vkt.NumberField(
        'Overall height', 
        suffix='m', 
        default=16,
        min=0
    )
    
    step2.solidity_ratio = vkt.NumberField(
        'Solidity ratio, ϕ', 
        default=0.30,
        min=0,
        max=1,
        num_decimals=2
    )
    step3 = vkt.Step("Structural Agent", views=["get_plotly_view","design_results_view"])
    step3.text1 = vkt.Text(dedent(
        """# Structural Agent

Talk to an **AI agent** to analyze the structural model, **get reaction loads**, assess foundation alternatives (pile foundation, monopile/caisson, and footing), then generate and update the CAD file and push it to ACC.
"""
    ))
    step3.chat = vkt.Chat("", method="call_llm")

class Controller(vkt.Controller):
    parametrization = Parametrization(width=40)

    @APSView("Model Viewer", duration_guess=40)
    def show_cad_model(self, params, **kwargs):
        # 1. Get 3‑legged token from VIKTOR integration
        integration = vkt.external.OAuth2Integration("aps-integration-1")
        token = integration.get_access_token()
        cad_dict = Controller.get_cad_files_dict(params, **kwargs)
        if cad_dict:
            cad_urn = cad_dict[params.step1.files]["urn"]
            viewer_urn = Controller.to_b64url(cad_urn)
            return APSresult(urn_b64=viewer_urn, token=token)
        return None

    def get_file_content(self, params, **kwargs):
        """Get raw binary content of the selected file."""
        token = get_aps_token()
        return aps_helpers.get_file_content(
            token=token,
            hub_name=params.step1.hubs,
            project_name=params.step1.project,
            subfolder_path=params.step1.subfolder_path,
            file_name=params.step1.agent_contenxt,
        )

    @staticmethod
    def store_file_in_app(params, **kwargs):
        token = get_aps_token()
        file_content = aps_helpers.get_file_content(
            token=token,
            hub_name=params.step1.hubs,
            project_name=params.step1.project,
            subfolder_path=params.step1.subfolder_path,
            file_name=params.step1.stored_file,
        )
        vkt.Storage().set(
            "ifc_model",
            data=vkt.File.from_data(file_content),
            scope="entity",
        )

    @staticmethod
    def get_cad_files_dict(params, **kwargs) -> None | dict[str, dict[str, str]]:
        token = get_aps_token()
        viewables = aps_helpers.get_all_cad_file_from_hub(token=token)
        return viewables

    @staticmethod
    def to_b64url(value: str) -> str:
        return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")

    def call_llm(self, params, **kwargs) -> vkt.ChatResult | None:
            """Multi-turn conversation between the user and the agent."""
            # Get conversation
            conversation_history = params.step3.chat.get_messages()
            #  Check if user uploaded an Excel Field
            if conversation_history:
                response = llm_response(
                    conversation_history=conversation_history,
                )

                if response:
                    llm_message, fig = execute_tool(response, conversation=conversation_history)
                    if fig:
                        print("Storing fig")
                        store_scene(fig)
                        get_visibility(params,**kwargs)
                    return vkt.ChatResult(params.step3.chat, llm_message)
                else:
                    raise ValueError("The LLM returned no parsed reponse.")
            return None

    @vkt.PlotlyView("Plotting Tool", width=100)
    def get_plotly_view(self, params, **kwargs) -> vkt.PlotlyResult:
        """This view plots the output of a tool call in a Plotly view.
        All tool calls are go.Figures exported as JSON. They are saved in
        Storage and retrieved here."""
        # 1. Delete tools calls from storage if there is no .xlsx file
        if not params.step3.chat:
            entities = vkt.Storage().list(scope="entity")
            for entity in entities:
                if entity == "view":
                    vkt.Storage().delete("view", scope="entity")

        # 2. Try to get the previous view from the tool call, otherwise blank scene
        try:
            raw = vkt.Storage().get("view", scope="entity").getvalue()
            fig = go.Figure(json.loads(raw))
        except Exception:
            fig = default_blank_scene()

        return vkt.PlotlyResult(fig.to_json())

    def send_data2acc(self, params, **kwargs):
        """Upload an IFC file to ACC in the folder selected in the UI."""
        # Get token and parameters from UI
        token = get_aps_token()
        print("hub", params.step1.hubs)
        print("project", params.step1.project)
        print("subfolder_path", params.step1.subfolder_path)
        # Get project and folder IDs
        hub_id = aps_helpers.get_hub_id_by_name(token, params.step1.hubs) # alejandroduartevendries@gmail.com
        project_id = aps_helpers.get_project_id_by_name(token, hub_id, params.step1.project) # Sample Project - Seaport Civic Center
        
        # Get folder ID for the selected subfolder path
        all_paths_with_ids = aps_helpers.get_all_folder_paths_with_ids(hub_id, project_id, token)
        folder_id = next((fid for path, fid in all_paths_with_ids if path == params.step1.subfolder_path), None) # Files/Structural
        
        if not folder_id:
            print(f"Could not find folder ID for path '{params.step1.subfolder_path}'")
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
        response = requests.post(storage_create_url, headers=headers, json=storage_payload)
        response.raise_for_status()
        storage_urn = response.json()["data"]["id"]
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


    @vkt.TableView("Optimization Results", visible=get_visibility)
    def design_results_view(self, params, **kwargs):
        get_visibility(params, **kwargs)

        try:
            raw_table_data = (
                vkt.Storage()
                   .get("optimization_table", scope="entity")
                   .getvalue()
            )
            table = json.loads(raw_table_data)
        except (FileNotFoundError, TypeError):
            return vkt.TableResult(
                data=[],
                column_headers=["No results generated yet."]
            )

        # 1. Extend headers with a Compliant column
        headers = table["headers"] + ["Compliant"]
        rows    = table["data"]
        flags   = table["flags"]

        styled_rows = []
        for row_vals, ok in zip(rows, flags):
            # 2. Keep original cells unstyled
            cells = list(row_vals)

            # 3. Create a styled Compliant cell only
            flag_text  = "Yes" if ok else "No"
            flag_style = {
                "background_color": vkt.Color.green() if ok else vkt.Color.red(),
                "text_style":      "bold",
            }
            cells.append(vkt.TableCell(flag_text, **flag_style))

            styled_rows.append(cells)

        # 4. Return the table: only the Compliant column is colored
        return vkt.TableResult(
            data=styled_rows,
            column_headers=headers
        )

    @vkt.MapView("Location Selection")
    def show_map(self, params, **kwargs):
        """Display map with selected location"""
        # Create a map point at the selected location
        map_point = vkt.MapPoint.from_geo_point(
            params.step2.location,
            title="Selected Location",
            description=f"Lat: {params.step2.location.lat:.4f}, Lon: {params.step2.location.lon:.4f}",
            color=vkt.Color(255, 0, 0)  # Red marker
        )
        
        return vkt.MapResult([map_point])

    @vkt.DataView("Wind Analysis Results")
    def calculate_wind_pressures(self, params, **kwargs):
        """Calculate wind pressures based on input parameters"""
        
        # Extract coordinates
        latitude = params.step2.location.lat
        longitude = params.step2.location.lon
        
        # Mock calculation coefficients (in real application, these would be calculated based on location and parameters)
        coefficients = {
            "Kz": 1.04,  # Velocity pressure exposure coefficient
            "Kd": 0.85,  # Directionality factor
            "Kzt": 1.00, # Topographic factor
            "Ke": 1.00,  # Air density factor
            "G": 0.85,   # Gust effect factor
            "Cf": 2.94   # Force coefficient for lattice framework
        }
        
        # Mock wind speeds and pressures (in real application, these would be calculated from wind data)
        ultimate_wind = {
            "V_ms": 50.96,
            "qz_kPa": 1.407,
            "p_kPa": 3.520
        }
        
        service_wind = {
            "y10": {"V_ms": 31.29, "qz_kPa": 0.531, "p_kPa": 1.326},
            "y25": {"V_ms": 34.42, "qz_kPa": 0.642, "p_kPa": 1.604},
            "y50": {"V_ms": 37.10, "qz_kPa": 0.746, "p_kPa": 1.864},
            "y100": {"V_ms": 39.79, "qz_kPa": 0.858, "p_kPa": 2.144}
        }
        
        # Create data groups for display
        data = vkt.DataGroup()
        
        # Location information
        location_group = vkt.DataGroup()
        location_group.add(
            vkt.DataItem("Latitude", f"{latitude:.4f}", suffix="°"),
            vkt.DataItem("Longitude", f"{longitude:.4f}", suffix="°")
        )
        data.add(vkt.DataItem("Location", subgroup=location_group))
        
        # Input parameters
        input_group = vkt.DataGroup()
        input_group.add(
            vkt.DataItem("Exposure Category", params.step2.exposure_category),
            vkt.DataItem("Risk Category", params.step2.risk_category),
            vkt.DataItem("Overall Height", params.step2.overall_height, suffix="m"),
            vkt.DataItem("Solidity Ratio (ϕ)", params.step2.solidity_ratio)
        )
        data.add(vkt.DataItem("Input Parameters", subgroup=input_group))
        
        # Coefficients
        coeff_group = vkt.DataGroup()
        coeff_group.add(
            vkt.DataItem("Kz (Velocity pressure exposure coefficient)", coefficients["Kz"]),
            vkt.DataItem("Kd (Directionality factor)", coefficients["Kd"]),
            vkt.DataItem("Kzt (Topographic factor)", coefficients["Kzt"]),
            vkt.DataItem("Ke (Air density factor)", coefficients["Ke"]),
            vkt.DataItem("G (Gust effect factor)", coefficients["G"]),
            vkt.DataItem("Cf (Force coefficient)", coefficients["Cf"])
        )
        data.add(vkt.DataItem("Coefficients", subgroup=coeff_group))
        
        # Ultimate wind
        ultimate_group = vkt.DataGroup()
        ultimate_group.add(
            vkt.DataItem("Wind Speed", ultimate_wind["V_ms"], suffix="m/s"),
            vkt.DataItem("Velocity Pressure", ultimate_wind["qz_kPa"], suffix="kPa"),
            vkt.DataItem("Design Pressure", ultimate_wind["p_kPa"], suffix="kPa")
        )
        data.add(vkt.DataItem("Ultimate Wind", subgroup=ultimate_group))
        
        # Service wind
        service_group = vkt.DataGroup()
        for period, values in service_wind.items():
            period_group = vkt.DataGroup()
            period_group.add(
                vkt.DataItem("Wind Speed", values["V_ms"], suffix="m/s"),
                vkt.DataItem("Velocity Pressure", values["qz_kPa"], suffix="kPa"),
                vkt.DataItem("Design Pressure", values["p_kPa"], suffix="kPa")
            )
            service_group.add(vkt.DataItem(f"{period.upper()} Year Return", subgroup=period_group))
        data.add(vkt.DataItem("Service Wind", subgroup=service_group))
        
        return vkt.DataResult(data)