from pydantic import BaseModel, Field
from typing import Union, Literal
from textwrap import dedent
import instructor
from openai import OpenAI
from dotenv import load_dotenv
from llm_eval.router import Router, evals as router_evals, format_prompt

# Import the structural tools
from app.llm_engine import PlotOpenSeesModelTool, RunOpenSeesModelTool, DisplayLoadsTool

load_dotenv()
client = instructor.from_openai(OpenAI())

class StructuralTools(BaseModel):
    response: str = Field(..., description="Be conversational, friendly and format the response always nicely")
    tool: Union[PlotOpenSeesModelTool, RunOpenSeesModelTool, DisplayLoadsTool] = Field(..., description="Select the appropriate structural tool based on the user request")
    why: str = Field(..., description="Explain why you selected this specific tool")

def structural_evals(conversation_history: list[dict]) -> StructuralTools:
    messages = []
    system_message = {
        "role": "system",
        "content": dedent(
            """
            You are a structural engineering assistant with access to OpenSees for structural analysis.
            Select the most appropriate tool to respond to the user's request:

            1. PlotOpenSeesModelTool: Use when the user wants to visualize the structural model
               geometry and sections without running analysis.

            2. RunOpenSeesModelTool: Use when the user wants to analyze the model, calculate reactions,
               displacements, or any other type of structural analysis.
               
            3. DisplayLoadsTool: Use when the user specifically wants to see or visualize the loads
               applied to the structural model.

            Be clear and friendly in your response, and explain why you selected the specific tool.
            """
        )
    }
    messages.append(system_message)
    messages.extend(conversation_history)
    
    return client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        response_model=StructuralTools,
    )

# Structural test cases
structural_test_cases = [
    {
        "prompt": "Hello, I am Thomas. Could you plot the model from the ACC data?",
        "expected_tool": PlotOpenSeesModelTool,
        "description": "Request to plot model"
    },
    {
        "prompt": "Display the loads from the critical load combination.",
        "expected_tool": DisplayLoadsTool,
        "description": "Request to display loads"
    },
    {
        "prompt": "Run the model and display deformations and reactions loads.",
        "expected_tool": RunOpenSeesModelTool,
        "description": "Request to run model and show deformations"
    }
]

def evaluate_structural_workflow():
    """
    First route the request through the router, then if categorized as 'Structural',
    pass to the structural tools evaluator.
    """
    results = []
    success_count = 0
    failure_count = 0
    
    # Terminal colors
    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"
    
    print("Running Structural Workflow evaluation...\n")
    
    for i, test_case in enumerate(structural_test_cases, 1):
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
        
        # Step 2: If categorized as structural, evaluate with structural tools
        if request_type == "Structural":
            structural_response = structural_evals(conversation_history=format_prompt(prompt))
            actual_tool = type(structural_response.tool)
            actual_tool_name = actual_tool.__name__
            
            print(f"Selected tool: {actual_tool_name}")
            print(f"Tool selection reasoning: {structural_response.why}")
            
            # Check if the correct tool was selected
            if isinstance(structural_response.tool, expected_tool):
                result = f"{GREEN}✓ SUCCESS{RESET}"
                success_count += 1
            else:
                result = f"{RED}✗ FAILURE{RESET}"
                failure_count += 1
        else:
            result = f"{RED}✗ FAILURE (Not routed to Structural){RESET}"
            actual_tool_name = "N/A - Not routed to Structural"
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
    print(f"Total tests: {len(structural_test_cases)}")
    print(f"Successful: {GREEN}{success_count} ✓{RESET}")
    print(f"Failed: {RED}{failure_count} ✗{RESET}")
    print(f"Success rate: {GREEN if success_count/len(structural_test_cases) >= 0.8 else RED}{success_count/len(structural_test_cases)*100:.1f}%{RESET}")
    
    # Print details of failures if any
    if failure_count > 0:
        print(f"\n{RED}Failed tests:{RESET}")
        for test in results:
            if test["result"] == "FAILURE":
                print(f"{RED}✗ Test {test['test_num']}: Expected {test['expected_tool']}, got {test['actual_tool']} - {test['description']}{RESET}")

if __name__ == "__main__":
    evaluate_structural_workflow()
