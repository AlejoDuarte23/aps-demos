from app.llm_engine import Response, PlotOpenSeesModelTool, DisplayLoadsTool, RunOpenSeesModelTool, GetGeotechnicalInputsForFoundationDesign, PullGeotechnicalReportTool, FootingDesignTool
from dotenv import load_dotenv
from openai import OpenAI
from textwrap import dedent
from pydantic import BaseModel, Field
import instructor
from typing import Any, Type, Literalf
from instructor.dsl.partial import PartialLiteralMixin

class Router(BaseModel, PartialLiteralMixin):
    request_type: Literal["Structural", "GeotechnicalInput", "GeotechnicalDesign"]


load_dotenv()
client = instructor.from_openai(OpenAI())

def evals(conversation_history: list[dict],
                 verbose: bool = True) -> Response:    
    messages = []
    # Default system prompt is always first
    system_message = {
        "role": "system",
        "content": dedent(
            """
            Context: You are interacting with an engineering system that has several modules:

            ==== STRUCTURAL ANALYSIS MODULE (OpenSees) ====[]
            OpenSees is an open-source package for structural modeling and analysis. You can:
            
            1. PlotOpenSeesModelTool: Use this when the user asks to view, display, or plot the structural model from ACC. 
               This tool DOES NOT run the model - it only shows the model geometry and sections!
            
            2. DisplayLoadsTool: Use this when the user wants to see, visualize, or display loads, 
               especially from critical load combinations.
            
            3. RunOpenSeesModelTool: Use this when the user wants to analyze the model, calculate reactions, 
               compute deformations, or run structural analysis for the OpenSees model.
            

            ==== GEOTECHNICAL DATA MODULE ====[]
            You can retrieve and process geotechnical information:
            
            4. PullGeotechnicalReportTool: Use this when the user specifically requests to access, retrieve, 
               or get geotechnical reports from ACC.

            5. GetGeotechnicalInputsForFoundationDesign: Use this when the user asks for soil parameters, 
               ground conditions, or geotechnical data needed for foundation design. This tool formats and 
               structures soil information from the chat context or from the output of PullGeotechnicalReportTool.
            

            ==== FOUNDATION DESIGN MODULE ====[]
            There are two types of foundations you can design:
            
            6. FootingDesignTool: Use this when the user wants to design, optimize, or calculate FOOTINGS 
               using allowable bearing pressures.
            
            7. PilesDesignTools: Use this when the user asks for "optimal design of the pile foundation" 
               or wants to design, optimize, or calculate PILES.
            

            IMPORTANT: Only select a tool when the user explicitly requests functionality related to it. 
            When a user makes a request, assume all dependencies are met and just call the appropriate tool. 
            For general conversation, informational questions, or greetings, don't select any tool (use None).

            IMPORTANT ALWAY USE TOOLS Do not Perform calcualtion or allucinate input data
            """
        )
    }
    messages.append(system_message)
    messages.extend(conversation_history)

    resp = client.chat.completions.create(
        model="o3",
        messages=messages,
        response_model=Response,
    )

    return resp


def format_prompt(query: str) -> list[dict[str, str]]:
    return [{"role":"user", "content": query}]


# Define test cases with expected tool type
test_cases = [
    {
        "prompt": "Hello how are you!",
        "expected_tool": None,
        "description": "Basic greeting"
    },
    {
        "prompt": "Hello, I am Thomas. Could you plot the model from the ACC data?",
        "expected_tool": PlotOpenSeesModelTool,
        "description": "Request to plot model"
    },
    {
        "prompt": "Great! Display the loads from the critical load combination.",
        "expected_tool": DisplayLoadsTool,
        "description": "Request to display loads"
    },
    {
        "prompt": "Run the model and display deformations and reactions loads.",
        "expected_tool": RunOpenSeesModelTool,
        "description": "Request to run model and show deformations"
    },
    {
        "prompt": "Please get the geotechnical report from my ACC hub. The name is \"GEOTECHNICAL DATA SUMMARY REV0.\"",
        "expected_tool": PullGeotechnicalReportTool,
        "description": "Request to get geotechnical report"
    },
    {
        "prompt": "Could you get the geotechnical inputs to design a pile foundation?",
        "expected_tool": GetGeotechnicalInputsForFoundationDesign,
        "description": "Request for geotechnical inputs for pile design"
    },
    {
        "prompt": "Let's proceed with the optimal design of the pile foundation",
        "expected_tool": FootingDesignTool,  # This might need to be adjusted based on your system
        "description": "Request to design pile foundation"
    },
    {
        "prompt": "Could you get the soil inputs prior to design a footing, including all the allowable bearing pressures based on embedment and footing size?",
        "expected_tool": GetGeotechnicalInputsForFoundationDesign,
        "description": "Request for soil inputs for footing design"
    },
    {
        "prompt": "Proceed with the design of a footing. Use the previously provided allowable bearing pressures at different embedment depths to determine the optimal one.",
        "expected_tool": FootingDesignTool,
        "description": "Request to design footing"
    }
]

if __name__ == "__main__":
    results = []
    success_count = 0
    failure_count = 0
    
    # Terminal colors
    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"
    
    print("Running LLM tool selection evaluations...\n")
    
    for i, test_case in enumerate(test_cases, 1):
        prompt = test_case["prompt"]
        expected_tool = test_case["expected_tool"]
        description = test_case["description"]
        
        print(f"Test {i}: {description}")
        print(f"Prompt: \"{prompt}\"")
        print(f"Expected tool: {expected_tool.__name__ if expected_tool else 'None'}")
        
        response = evals(conversation_history=format_prompt(prompt))
        # Print the raw response object
        print(f"Raw response: {response}")
        
        actual_tool_type = type(response.selected_tool)
        actual_tool_name = actual_tool_type.__name__ if response.selected_tool is not None else "None"
        
        # Print the LLM's reasoning for tool selection
        reasoning = getattr(response, 'why', 'No reasoning provided')
        print(f"LLM reasoning: {reasoning}")
        
        # Check if the expected tool matches the actual tool
        if (expected_tool is None and response.selected_tool is None) or \
           (expected_tool is not None and isinstance(response.selected_tool, expected_tool)):
            result = f"{GREEN}✓ SUCCESS{RESET}"
            success_count += 1
        else:
            result = f"{RED}✗ FAILURE{RESET}"
            failure_count += 1
            
        print(f"Actual tool: {actual_tool_name}")
        print(f"Result: {result}\n")
        
        # Include reasoning in results
        results.append({
            "test_num": i,
            "description": description,
            "expected": expected_tool.__name__ if expected_tool else "None",
            "actual": actual_tool_name,
            "reasoning": reasoning,
            "result": "SUCCESS" if "SUCCESS" in result else "FAILURE"
        })
    
    # Print summary
    print("\n=== EVALUATION SUMMARY ===")
    print(f"Total tests: {len(test_cases)}")
    print(f"Successful: {GREEN}{success_count} ✓{RESET}")
    print(f"Failed: {RED}{failure_count} ✗{RESET}")
    print(f"Success rate: {GREEN if success_count/len(test_cases) >= 0.8 else RED}{success_count/len(test_cases)*100:.1f}%{RESET}")
    
    # Print details of failures if any
    if failure_count > 0:
        print(f"\n{RED}Failed tests:{RESET}")
        for test in results:
            if test["result"] == "FAILURE":
                print(f"{RED}✗ Test {test['test_num']}: Expected {test['expected']}, got {test['actual']} - {test['description']}{RESET}")