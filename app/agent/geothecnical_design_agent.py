from pydantic import BaseModel, Field
from typing import Union
from textwrap import dedent
import instructor
from openai import OpenAI
from dotenv import load_dotenv

# Import the geotechnical design tools
from app.foundations.piles.piles import PilesDesignTools
from app.tools import FootingDesignTool, UploadToAccTool
load_dotenv()
client = instructor.from_openai(OpenAI())

class GeotechnicalDesignTools(BaseModel):
    response: str = Field(..., description="Be conversational, friendly and format the response always nicely")
    tool: Union[FootingDesignTool, PilesDesignTools,UploadToAccTool, None] = Field(..., description="Select the appropriate geotechnical design tool based on the user request")
    why: str = Field(..., description="Explain why you selected this specific tool")

def geotechnical_design_agent(conversation_history: list[dict]) -> GeotechnicalDesignTools:
    messages = []
    system_message = {
        "role": "system",
        "content": dedent(
            """
            You are a geotechnical engineering assistant that helps users design foundations.
            Select the most appropriate tool to respond to the user's request:

            1. FootingDesignTool: Use when the user wants to design a shallow foundation like a footing
               based on previously provided soil parameters and allowable bearing pressures.

            2. PileDesignTool: Use when the user wants to design a deep foundation like piles
               based on previously provided soil parameters.

            3. UploadToAccTool: The optimal Design can be push or send back to ACC autodesk platform services. User my say something like "Upload the model to ACC".

            Be clear and friendly in your response, and explain why you selected the specific tool.
            """
        )
    }
    messages.append(system_message)
    messages.extend(conversation_history)
    
    return client.chat.completions.create(
        model="gpt-4.1",
        messages=messages,
        response_model=GeotechnicalDesignTools,
    )
