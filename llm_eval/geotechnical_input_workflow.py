from pydantic import BaseModel, Field
from typing import Union, Literal
from textwrap import dedent
import instructor
from instructor.dsl.partial import PartialLiteralMixin
from openai import OpenAI
from dotenv import load_dotenv
from llm_eval.router import Router, evals as router_evals, format_prompt

# Import the geotechnical input tools
from app.llm_engine import GetGeotechnicalInputsForFoundationDesign, PullGeotechnicalReportTool

load_dotenv()
client = instructor.from_openai(OpenAI())

class GetGeotechnicalInputsForFoundationDesign2(BaseModel,PartialLiteralMixin):
    design_type: Literal["pile","footing"]

class GeotechnicalInputTools(BaseModel):
    response: str = Field(..., description="Be conversational, friendly and format the response always nicely")
    tool: Union[GetGeotechnicalInputsForFoundationDesign2, PullGeotechnicalReportTool, None] = Field(..., description="Select the appropriate geotechnical input tool based on the user request")
    why: str = Field(..., description="Explain why you selected this specific tool")

def geotechnical_input_evals(conversation_history: list[dict]) -> GeotechnicalInputTools:
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

# Geotechnical input test cases
geotechnical_input_test_cases = [
    {
        "prompt": "Please get the geotechnical report from my ACC hub. The name is \"GEOTECHNICAL DATA SUMMARY REV0.\"",
        "expected_tool": PullGeotechnicalReportTool,
        "description": "Request to get geotechnical report"
    },
    {
        "prompt": "Could you get the geotechnical inputs to design a pile foundation?",
        "expected_tool": GetGeotechnicalInputsForFoundationDesign2,
        "description": "Request for geotechnical inputs for pile design"
    },
    {
        "prompt": "Could you get the soil inputs prior to design a footing, including all the allowable bearing pressures based on embedment and footing size?",
        "expected_tool": GetGeotechnicalInputsForFoundationDesign2,
        "description": "Request for soil inputs for footing design"
    }
]

def execute_inputs_for_foundation(input= GetGeotechnicalInputsForFoundationDesign2):

    context = """ 
        Soil type: Medium-dense silty sand (SP–SM)
        Unit weight (γ): 19 kN/m³
        Angle of shearing resistance (φ): 34°
        Cohesion (c): 0 kPa (non-cohesive sand)
        Young’s modulus (E): 30 MPa
        Representative cone resistance (qc): 10 MPa
        Unit base resistance (qb): 6000 kPa
        Unit shaft resistance (fs): 200 kPa
        Safety factor: 2.0 (applied to total pile resistance)
        Water table: Not encountered (drained conditions assumed)
        • Unit weight γ: 19 kN/m³ • Friction angle φ′: 34° • Cohesion c′: 0 kPa • Elastic modulus E: 30 MPa

        Allowable bearing pressures qadm for square footings (FS = 3), for various embedment depths (Df) and footing widths (B):

        Depth Df (m) vs Width B (m) → qadm (kPa)

        Df = 2.0 m → B = 2.0: 65 | 2.5: 75 | 3.0: 85 | 3.5: 90 | 4.0: 95 Df = 2.5 m → B = 2.0: 80 | 2.5: 98 | 3.0: 110 | 3.5: 120 | 4.0: 130 Df = 3.0 m → B = 2.0: 100 | 2.5: 120 | 3.0: 140 | 3.5: 155 | 4.0: 165 Df = 3.5 m → B = 2.0: 115 | 2.5: 135 | 3.0: 155 | 3.5: 165 | 4.0: 170

        Service settlement allowance: 25 mm (working loads)
        """
    from app.llm_engine import FootingSoilData
    from app.foundations.piles.piles import PileSoilData
    
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

def evaluate_geotechnical_input_workflow():
    """
    First route the request through the router, then if categorized as 'GeotechnicalInput',
    pass to the geotechnical input tools evaluator.
    """
    results = []
    success_count = 0
    failure_count = 0
    
    # Terminal colors
    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"
    
    print("Running Geotechnical Input Workflow evaluation...\n")
    
    for i, test_case in enumerate(geotechnical_input_test_cases, 1):
        prompt = test_case["prompt"]
        expected_tool = test_case["expected_tool"]
        description = test_case["description"]
        
        print(f"Test {i}: {description}")
        print(f"Prompt: \"{prompt}\"")
        print(f"Expected tool: {expected_tool.__name__}")
        
        # Step 1: Route the request
        route_response = router_evals(conversation_history=format_prompt(prompt))
        request_type = route_response.request_type
        
        print(f"Router classification: {request_type}")
        print(f"Router reasoning: {route_response.reason}")
        
        # Step 2: If categorized as geotechnical input, evaluate with geotechnical input tools
        if request_type == "GeotechnicalInput":
            geo_response = geotechnical_input_evals(conversation_history=format_prompt(prompt))
            actual_tool = type(geo_response.tool)
            actual_tool_name = actual_tool.__name__
            
            print(f"Selected tool: {actual_tool_name}")
            print(f"Tool selection reasoning: {geo_response.why}")
            
            # Step 3: Execute foundation design inputs if GetGeotechnicalInputsForFoundationDesign2 is selected
            if isinstance(geo_response.tool, GetGeotechnicalInputsForFoundationDesign2):
                print(f"\nExecuting foundation design inputs for {geo_response.tool.design_type} design...")
                execution_result = execute_inputs_for_foundation(geo_response.tool)
                print(f"Foundation design inputs processed. Response: {execution_result.response}")
            
            # Check if the correct tool was selected
            if isinstance(geo_response.tool, expected_tool):
                result = f"{GREEN}✓ SUCCESS{RESET}"
                success_count += 1
            else:
                result = f"{RED}✗ FAILURE{RESET}"
                failure_count += 1
        else:
            result = f"{RED}✗ FAILURE (Not routed to GeotechnicalInput){RESET}"
            actual_tool_name = "N/A - Not routed to GeotechnicalInput"
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
    print(f"Total tests: {len(geotechnical_input_test_cases)}")
    print(f"Successful: {GREEN}{success_count} ✓{RESET}")
    print(f"Failed: {RED}{failure_count} ✗{RESET}")
    print(f"Success rate: {GREEN if success_count/len(geotechnical_input_test_cases) >= 0.8 else RED}{success_count/len(geotechnical_input_test_cases)*100:.1f}%{RESET}")
    
    # Print details of failures if any
    if failure_count > 0:
        print(f"\n{RED}Failed tests:{RESET}")
        for test in results:
            if test["result"] == "FAILURE":
                print(f"{RED}✗ Test {test['test_num']}: Expected {test['expected_tool']}, got {test['actual_tool']} - {test['description']}{RESET}")

if __name__ == "__main__":
    evaluate_geotechnical_input_workflow()
