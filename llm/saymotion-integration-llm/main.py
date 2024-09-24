import requests
import os
import openai

DEEP_MOTION_API_KEY = os.getenv("DEEP_MOTION_API_KEY")

openai.api_key = os.getenv("OPENAI_API_KEY")

# Base URL for DeepMotion API
BASE_URL = "https://api.deepmotion.com/v1/saymotion"

# Headers for authentication
headers = {
    'Authorization': f'Bearer {DEEP_MOTION_API_KEY}',
    'Content-Type': 'application/json'
}

#Generating 3D Poses Based on Prompts

# Function to generate pose commands based on a natural language prompt
def generate_pose_command(prompt):
    response = openai.Completion.create(
        engine="gpt-4",  # Use appropriate LLM model
        prompt=f"Translate the following prompt into a 3D pose instruction for a character: {prompt}",
        max_tokens=100,
        n=1,
        stop=None,
        temperature=0.7
    )
    
    return response['choices'][0]['text'].strip()

#Sending Pose Instructions to DeepMotion

def create_3d_pose(pose_instruction):
    # Define the request payload
    payload = {
        "pose": pose_instruction,
        "model_id": "******"  # 3D model ID
    }
    
    # Send the request to create a pose
    response = requests.post(f"{BASE_URL}/poses", headers=headers, json=payload)
    
    if response.status_code == 200:
        print("Pose created successfully!")
        return response.json()
    else:
        print(f"Failed to create pose: {response.text}")
        return None

def process_pose_creation(prompt):
    # Generate pose instruction from the user's prompt like Make the character jump with both arms raised.
    pose_instruction = generate_pose_command(prompt)
    print(f"Generated Pose Instruction: {pose_instruction}")
    
    # Create the 3D pose on DeepMotion's platform
    pose_response = create_3d_pose(pose_instruction)
    
    if pose_response:
        print(f"Pose ID: {pose_response['id']}")
        return pose_response
    else:
        print("Failed to create the pose.")
        return None
