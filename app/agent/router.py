import instructor

from dotenv import load_dotenv
from openai import OpenAI
from textwrap import dedent
from pydantic import BaseModel, Field
from typing import Literal
from instructor.dsl.partial import PartialLiteralMixin

class Router(BaseModel, PartialLiteralMixin):
    request_type: Literal["Structural", "GeotechnicalInput", "GeotechnicalDesign", "None"]
    reason: str = Field(..., description="Explain why you chose this request type")

load_dotenv()
client = instructor.from_openai(OpenAI())

def agent_router(conversation_history: list[dict]) -> Router:
    messages = []
    system_message = {
        "role": "system",
        "content": dedent(
            """
            You are a router for an engineering assistant that categorizes user requests.
            
            Categorize each user request into one of these categories:
            1. "Structural": Requests related to structural analysis, visualization of structural models, 
               displaying loads, or running structural simulations using OpenSees.
            
            2. "GeotechnicalInput": Requests for retrieving, formatting, or processing geotechnical data 
               such as soil reports, soil parameters, or ground conditions.
            
            3. "GeotechnicalDesign": Requests specifically about designing foundations (footings or piles) 
               using previously obtained geotechnical data.
            
            4. "None": General conversation, greetings, or questions not related to the above categories.
            
            Always provide a reason for your classification.
            """
        )
    }
    messages.append(system_message)
    messages.extend(conversation_history)
    
    return client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        response_model=Router,
    )
