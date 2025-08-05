import instructor

from pydantic import BaseModel, Field
from typing import Union
from textwrap import dedent
from app.foundations.footings.footings import FootingSoilData
from app.foundations.piles.piles import PileSoilData
from openai import OpenAI
from dotenv import load_dotenv
# Import the geotechnical input tools
from app.tools import GetGeotechnicalInputsForFoundationDesign, PullGeotechnicalReportTool

load_dotenv()
client = instructor.from_openai(OpenAI())

class GeotechnicalInputTools(BaseModel):
    response: str = Field(..., description="Be conversational, friendly and format the response always nicely")
    tool: Union[GetGeotechnicalInputsForFoundationDesign, PullGeotechnicalReportTool, None] = Field(..., description="Select the appropriate geotechnical input tool based on the user request")
    why: str = Field(..., description="Explain why you selected this specific tool")

def geothecnical_input_agent(conversation_history: list[dict]) -> GeotechnicalInputTools:
    messages = []
    system_message = {
        "role": "system",
        "content": dedent(
            """
            You are a geotechnical engineering assistant that helps users retrieve and process soil data.
            Select the most appropriate tool to respond to the user's request:

            1. PullGeotechnicalReportTool: Use when the user wants to retrieve a geotechnical report 
               from Autodesk Construction Cloud (ACC). This tool fetches the actual document.

            2. GetGeotechnicalInputsForFoundationDesign: Use when the user wants to extract, format, 
               or process soil parameters from previously obtained reports for use in foundation design.
               This tool helps organize the data into the correct format.

            Be clear and friendly in your response, and explain why you selected the specific tool.
            """
        )
    }
    messages.append(system_message)
    messages.extend(conversation_history)
    
    return client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        response_model=GeotechnicalInputTools,
    )



def execute_inputs_for_foundation(input:GetGeotechnicalInputsForFoundationDesign, context:str):
    
    response_model = FootingSoilData if input.design_type == "footing" else PileSoilData
    inputs = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role":"user", "content":f"This is the raw geotechnical information {context}"}],
        response_model=response_model,
    )

    return client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role":"user", "content":f"do no use tool just let the user know the inputs and ask them if they want to proceed with the design, user need Complete information do not omit anithing! {inputs}"}],
        response_model=GeotechnicalInputTools,
    )
