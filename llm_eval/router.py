# from app.llm_engine import (
#     Response,
#     PlotOpenSeesModelTool,
#     DisplayLoadsTool,
#     RunOpenSeesModelTool,
#     GetGeotechnicalInputsForFoundationDesign,
#     PullGeotechnicalReportTool,
#     FootingDesignTool,
# )
from dotenv import load_dotenv
from openai import OpenAI
from textwrap import dedent
from pydantic import BaseModel, Field
import instructor
from typing import Any, Type, Literal, Union
from instructor.dsl.partial import PartialLiteralMixin


class Router(BaseModel, PartialLiteralMixin):
    request_type: Literal["Structural", "GeotechnicalInput", "GeotechnicalDesign", "None"]
    reason: str = Field(..., description="Explain why you chose this request type")


load_dotenv()
client = instructor.from_openai(OpenAI())


def evals(conversation_history: list[dict]) -> Router:
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


def format_prompt(query: str) -> list[dict[str, str]]:
    return [{"role": "user", "content": query}]


# Update test cases to use the request_type instead of expected_tool
test_cases = [
    {
        "prompt": "Hello how are you!",
        "expected": "None",
        "description": "Basic greeting"
    },
    {
        "prompt": "Hello, I am Thomas. Could you plot the model from the ACC data?",
        "expected": "Structural",
        "description": "Request to plot model"
    },
    {
        "prompt": "Great! Display the loads from the critical load combination.",
        "expected": "Structural",
        "description": "Request to display loads"
    },
    {
        "prompt": "Run the model and display deformations and reactions loads.",
        "expected": "Structural",
        "description": "Request to run model and show deformations"
    },
    {
        "prompt": "Please get the geotechnical report from my ACC hub. The name is \"GEOTECHNICAL DATA SUMMARY REV0.\"",
        "expected": "GeotechnicalInput",
        "description": "Request to get geotechnical report"
    },
    {
        "prompt": "Could you get the geotechnical inputs to design a pile foundation?",
        "expected": "GeotechnicalInput",
        "description": "Request for geotechnical inputs for pile design"
    },
    {
        "prompt": "Let's proceed with the optimal design of the pile foundation",
        "expected": "GeotechnicalDesign",
        "description": "Request to design pile foundation"
    },
    {
        "prompt": "Could you get the soil inputs prior to design a footing, including all the allowable bearing pressures based on embedment and footing size?",
        "expected": "GeotechnicalInput",
        "description": "Request for soil inputs for footing design"
    },
    {
        "prompt": "Proceed with the design of a footing. Use the previously provided allowable bearing pressures at different embedment depths to determine the optimal one.",
        "expected": "GeotechnicalDesign",
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
    
    print("Running Router evaluation...\n")
    
    for i, test_case in enumerate(test_cases, 1):
        prompt = test_case["prompt"]
        expected = test_case["expected"]
        description = test_case["description"]
        
        print(f"Test {i}: {description}")
        print(f"Prompt: \"{prompt}\"")
        print(f"Expected category: {expected}")
        
        response = evals(conversation_history=format_prompt(prompt))
        actual = response.request_type
        
        # Check if the expected category matches the actual category
        if expected == actual:
            result = f"{GREEN}✓ SUCCESS{RESET}"
            success_count += 1
        else:
            result = f"{RED}✗ FAILURE{RESET}"
            failure_count += 1
            
        print(f"Actual category: {actual}")
        print(f"Reason: {response.reason}")
        print(f"Result: {result}\n")
        
        results.append({
            "test_num": i,
            "description": description,
            "expected": expected,
            "actual": actual,
            "reason": response.reason,
            "result": "SUCCESS" if expected == actual else "FAILURE"
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
                print(f"  Reason given: {test['reason']}")