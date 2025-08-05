from pydantic import BaseModel, Field
from typing import Union, Literal
from textwrap import dedent
import instructor
from instructor.dsl.partial import PartialLiteralMixin
from openai import OpenAI
from dotenv import load_dotenv
from llm_eval.router import Router, evals as router_evals, format_prompt

# Import the geotechnical design tools
from app.llm_engine import FootingDesignTool
from app.foundations.piles.piles import PilesDesignTools

load_dotenv()
client = instructor.from_openai(OpenAI())

class GeotechnicalDesignTools(BaseModel):
    response: str = Field(..., description="Be conversational, friendly and format the response always nicely")
    tool: Union[FootingDesignTool, PilesDesignTools, None] = Field(..., description="Select the appropriate geotechnical design tool based on the user request")
    why: str = Field(..., description="Explain why you selected this specific tool")

def geotechnical_design_evals(conversation_history: list[dict]) -> GeotechnicalDesignTools:
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

# Update geotechnical design test cases to include full conversation history
geotechnical_design_test_cases = [
    {
        "conversation": [
            {
                "role": "assistant",
                "content": """Based on the information you provided, here's a detailed summary of the inputs needed for a footing design:

1. **Bearing Capacity Table:**
   - `Df=2.0`, `B=2.0`, `qadm=65.0`
   - `Df=2.0`, `B=2.5`, `qadm=75.0`
   - `Df=2.0`, `B=3.0`, `qadm=85.0`
   - `Df=2.0`, `B=3.5`, `qadm=90.0`
   - `Df=2.0`, `B=4.0`, `qadm=95.0`
   - `Df=2.5`, `B=2.0`, `qadm=80.0`
   - `Df=2.5`, `B=2.5`, `qadm=98.0`
   - `Df=2.5`, `B=3.0`, `qadm=110.0`

2. **Soil Properties:**
   - Unit Weight (Gamma): `19.0 kN/m³`
   - Angle of Internal Friction (Phi): `34.0°`
   - Cohesion (c): `0.0 kPa`

3. **Modulus of Elasticity:**
   - `E = 30.0 MPa`""",
            },
            {
                "role": "user",
                "content": "Proceed with the design of a footing. Use the previously provided allowable bearing pressures at different embedment depths to determine the optimal one."
            }
        ],
        "expected_tool": FootingDesignTool,
        "description": "Request to design footing with bearing capacity table context"
    },
    {
        "conversation": [
            {
                "role": "assistant",
                "content": """
Here's a summary of the inputs you've provided:

- **Unit Weight of Soil (γ)**: 19.0 kN/m³
- **Angle of Internal Friction (φ)**: 34.0 degrees
- **Cohesion (c)**: 0.0 kPa
- **Factor of Safety (FS)**: 200.0 (this seems very high, please double-check as typical values range from 1.5 to 3.0)
- **Bearing Capacity of Soil at Base (qb)**: 6000.0 kPa

Would you like to proceed with a foundation design based on these parameters? If so, please specify if you would like a pile or footing design, and I can assist you further."""
            },
            {
                "role": "user", 
                "content": "Let's proceed with the optimal design of the pile foundation"
            }
        ],
        "expected_tool": PilesDesignTools,
        "description": "Request to design pile foundation with soil parameters context"
    }
]

def execute_foundation_design(design_tool, soil_data):
    """
    Execute the foundation design based on the selected tool and soil data.
    This function would contain the actual design logic or call to specialized design functions.
    """
    # This is a placeholder for the actual implementation
    if isinstance(design_tool, FootingDesignTool):
        design_result = "Footing design completed with optimal dimensions"
    else:  # PileDesignTool
        design_result = "Pile foundation design completed with optimal configuration"
        
    return client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role":"user", "content":f"Present the foundation design results to the user: {design_result}"}],
        response_model=GeotechnicalDesignTools,
        max_retries=5
    )

def evaluate_geotechnical_design_workflow():
    """
    First route the request through the router, then if categorized as 'GeotechnicalDesign',
    pass to the geotechnical design tools evaluator.
    """
    results = []
    success_count = 0
    failure_count = 0
    
    # Terminal colors
    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"
    
    print("Running Geotechnical Design Workflow evaluation...\n")
    
    for i, test_case in enumerate(geotechnical_design_test_cases, 1):
        conversation = test_case["conversation"]
        expected_tool = test_case["expected_tool"]
        description = test_case["description"]
        
        # Get the last user message for routing
        last_user_message = None
        for msg in reversed(conversation):
            if msg["role"] == "user":
                last_user_message = msg["content"]
                break
        
        if not last_user_message:
            print(f"{RED}Error: No user message found in conversation for test {i}{RESET}")
            continue
        
        print(f"Test {i}: {description}")
        print(f"Last user message: \"{last_user_message}\"")
        print(f"Expected tool: {expected_tool.__name__}")
        
        # Step 1: Route the request based on the last user message
        route_response = router_evals(conversation_history=format_prompt(last_user_message))
        request_type = route_response.request_type
        
        print(f"Router classification: {request_type}")
        print(f"Router reasoning: {route_response.reason}")
        
        # Step 2: If categorized as geotechnical design, evaluate with geotechnical design tools
        if request_type == "GeotechnicalDesign":
            # Pass the full conversation history to the geotechnical design evaluator
            geo_design_response = geotechnical_design_evals(conversation_history=conversation)
            actual_tool = type(geo_design_response.tool)
            actual_tool_name = actual_tool.__name__
            
            print(f"Selected tool: {actual_tool_name}")
            print(f"Tool selection reasoning: {geo_design_response.why}")
            
            # Step 3: Execute foundation design if appropriate tool is selected
            # Extract soil data from conversation context
            soil_data = "Extracted from conversation context"  # In a real implementation, extract the actual data
            if geo_design_response.tool is not None:
                print(f"\nExecuting foundation design for {actual_tool_name}...")
                design_result = execute_foundation_design(geo_design_response.tool, soil_data)
                print(f"Design completed. Response: {design_result.response}")
            
            # Check if the correct tool was selected
            if isinstance(geo_design_response.tool, expected_tool):
                result = f"{GREEN}✓ SUCCESS{RESET}"
                success_count += 1
            else:
                result = f"{RED}✗ FAILURE{RESET}"
                failure_count += 1
        else:
            result = f"{RED}✗ FAILURE (Not routed to GeotechnicalDesign){RESET}"
            actual_tool_name = "N/A - Not routed to GeotechnicalDesign"
            failure_count += 1
            
        print(f"Result: {result}\n")
        
        results.append({
            "test_num": i,
            "description": description,
            "router_classification": request_type,
            "expected_tool": expected_tool.__name__,
            "actual_tool": actual_tool_name,
            "result": "SUCCESS" if result.find("SUCCESS") >= 0 else "FAILURE"
        })
    
    # Print summary
    print("\n=== EVALUATION SUMMARY ===")
    print(f"Total tests: {len(geotechnical_design_test_cases)}")
    print(f"Successful: {GREEN}{success_count} ✓{RESET}")
    print(f"Failed: {RED}{failure_count} ✗{RESET}")
    print(f"Success rate: {GREEN if success_count/len(geotechnical_design_test_cases) >= 0.8 else RED}{success_count/len(geotechnical_design_test_cases)*100:.1f}%{RESET}")
    
    # Print details of failures if any
    if failure_count > 0:
        print(f"\n{RED}Failed tests:{RESET}")
        for test in results:
            if test["result"] == "FAILURE":
                print(f"{RED}✗ Test {test['test_num']}: Expected {test['expected_tool']}, got {test['actual_tool']} - {test['description']}{RESET}")

if __name__ == "__main__":
    evaluate_geotechnical_design_workflow()
