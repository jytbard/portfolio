from transformers import pipeline, AutoModelForCausalLM, AutoTokenizer
import librosa
import numpy as np
import soundfile as sf

# Initialize Parler-TTS model
tts = pipeline("text-to-speech", model="Parler-TTS", device=0)

# Function to generate vocals from text

def generate_vocals(text, output_path="output_vocals.wav"):

    tts_result = tts(text)

    with open(output_path, 'wb') as f:

        f.write(tts_result['waveform'])

    print(f"Vocals saved to {output_path}")

lyrics = "The stars are shining bright tonight, a symphony of light."

generate_vocals(lyrics)

# Load MusicGen model and tokenizer

model = AutoModelForCausalLM.from_pretrained("facebook/MusicGen")

tokenizer = AutoTokenizer.from_pretrained("facebook/MusicGen")

# Function to generate music based on a text prompt (e.g., description of music)

def generate_music(prompt, max_length=500):

    inputs = tokenizer(prompt, return_tensors="pt")

    outputs = model.generate(inputs["input_ids"], max_length=max_length)

    return tokenizer.decode(outputs[0], skip_special_tokens=True)

prompt = "Create an upbeat background track with a soft melody and dynamic rhythm."

generated_music = generate_music(prompt)

print("Generated music sequence:", generated_music)

# Load vocals using librosa

def load_vocals(filepath):

    vocals, sr = librosa.load(filepath, sr=None)

    return vocals, sr

# Extract features from vocals

def extract_vocal_features(vocals, sr):

    tempo, _ = librosa.beat.beat_track(y=vocals, sr=sr)

    pitch = np.mean(librosa.feature.rms(y=vocals))

    return tempo, pitch

def generate_music_with_vocals(vocal_file, description, max_length=500):

    # Load and process vocals

    vocals, sr = load_vocals(vocal_file)

    tempo, pitch = extract_vocal_features(vocals, sr)
    
    # Adjust the prompt based on vocal features

    prompt = f"{description} with a tempo of {tempo} BPM and a pitch average of {pitch}."

    # Generate music using MusicGen

    generated_music = generate_music(prompt, max_length)

    return generated_music

vocal_file = "output_vocals.wav"

music_description = "Create an ambient background track that matches the vocals."

generated_music = generate_music_with_vocals(vocal_file, music_description)

print("Generated music sequence conditioned on vocals:", generated_music)

# Function to combine vocals and generated music into one audio file

def combine_audio(vocals_path, music_path, output_path="final_output.wav"):

    vocals, sr_vocals = librosa.load(vocals_path, sr=None)

    music, sr_music = librosa.load(music_path, sr=None)
    
    # Ensure both audio files have the same sampling rate

    if sr_vocals != sr_music:

        raise ValueError("Sampling rates of vocals and music must match!")
    
    # Mix the audio signals

    combined_audio = vocals + music 

    sf.write(output_path, combined_audio, sr_vocals)

    print(f"Final audio saved to {output_path}")

combine_audio("output_vocals.wav", "generated_music.wav")