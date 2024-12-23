#I used the following code snippet to fine-tune a Coca model using a Conch encoder, by working with pairs of histopathology images and their corresponding descriptions to enhance the model performance, for USA based diagnostic center

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from torchvision import transforms
from PIL import Image
import os
from transformers import AutoTokenizer, AutoModel
from coca_pytorch import CoCa

def get_device():
    return torch.device('cuda' if torch.cuda.is_available() else 'cpu')

# Step 1: Define the dataset
class HistopathologyDataset(Dataset):
    def __init__(self, image_dir, caption_file, transform=None):
        self.image_dir = image_dir
        self.transform = transform
        self.images = []
        self.captions = []

        with open(caption_file, 'r') as f:
            for line in f:
                image_name, caption = line.strip().split('\t')
                self.images.append(os.path.join(image_dir, image_name))
                self.captions.append(caption)

    def __len__(self):
        return len(self.images)

    def __getitem__(self, idx):
        image = Image.open(self.images[idx]).convert('RGB')
        caption = self.captions[idx]

        if self.transform:
            image = self.transform(image)

        return image, caption

# Step 2: Define the Conch encoder
class ConchEncoder(nn.Module):
    def __init__(self, pretrained_model_name='microsoft/BiomedVLP-CXR-BERT-general'):
        super(ConchEncoder, self).__init__()
        self.model = AutoModel.from_pretrained(pretrained_model_name)
        self.tokenizer = AutoTokenizer.from_pretrained(pretrained_model_name)

    def forward(self, text):
        inputs = self.tokenizer(text, return_tensors='pt', padding=True, truncation=True, max_length=512).to(get_device())
        outputs = self.model(**inputs)
        return outputs.last_hidden_state[:, 0, :]  # Use [CLS] token embedding

# Step 3: Initialize the CoCa model with Conch encoder
def initialize_coca_model(image_size, num_tokens, dim, conch_encoder):
    return CoCa(
        dim=dim,
        img_encoder=nn.Sequential(
            nn.Conv2d(3, 64, kernel_size=7, stride=2, padding=3),
            nn.ReLU(inplace=True),
            nn.MaxPool2d(kernel_size=3, stride=2, padding=1),
            nn.Conv2d(64, 192, kernel_size=3, padding=1),
            nn.ReLU(inplace=True),
            nn.MaxPool2d(kernel_size=3, stride=2, padding=1),
            nn.Conv2d(192, 384, kernel_size=3, padding=1),
            nn.ReLU(inplace=True),
            nn.Conv2d(384, 256, kernel_size=3, padding=1),
            nn.ReLU(inplace=True),
            nn.Conv2d(256, 256, kernel_size=3, padding=1),
            nn.ReLU(inplace=True),
            nn.MaxPool2d(kernel_size=3, stride=2, padding=1),
            nn.AdaptiveAvgPool2d((1, 1)),
            nn.Flatten(),
            nn.Linear(256, dim)
        ),
        text_encoder=conch_encoder,
        num_tokens=num_tokens,
        unimodal_depth=6,
        multimodal_depth=6,
        dim_head=64,
        heads=8,
        ff_mult=4
    )

# Step 4: Training loop with mixed precision
def train(model, train_loader, optimizer, criterion, device, num_epochs):
    scaler = torch.cuda.amp.GradScaler()  # Mixed precision scaler
    model.train()
    for epoch in range(num_epochs):
        for batch_idx, (images, captions) in enumerate(train_loader):
            images = images.to(device)
            captions = captions.to(device)

            optimizer.zero_grad()
            with torch.cuda.amp.autocast():
                loss = model(images, captions, return_loss=True)
            scaler.scale(loss).backward()
            scaler.step(optimizer)
            scaler.update()

            if batch_idx % 100 == 0:
                print(f"Epoch {epoch+1}/{num_epochs}, Batch {batch_idx}/{len(train_loader)}, Loss: {loss.item():.4f}")

# Step 5: Main function to run the fine-tuning process
def main():
    # Hyperparameters
    image_size = 224
    batch_size = 32
    num_epochs = 10
    learning_rate = 1e-4
    dim = 512
    num_tokens = 20000

    # Set up device
    device = get_device()

    # Define transforms
    transform = transforms.Compose([
        transforms.Resize((image_size, image_size)),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
    ])

    # Create dataset and dataloader
    dataset = HistopathologyDataset('***', '***/file.txt', transform=transform)
    train_loader = DataLoader(dataset, batch_size=batch_size, shuffle=True, num_workers=4, pin_memory=True, persistent_workers=True)

    # Initialize Conch encoder
    conch_encoder = ConchEncoder()

    # Initialize CoCa model
    model = initialize_coca_model(image_size, num_tokens, dim, conch_encoder)
    model = model.to(device)

    # Define optimizer and loss function
    optimizer = optim.AdamW(model.parameters(), lr=learning_rate)
    criterion = nn.CrossEntropyLoss()

    # Train the model
    train(model, train_loader, optimizer, criterion, device, num_epochs)

    # Save the fine-tuned model
    torch.save(model.state_dict(), 'coca_finetuned_histopathology.pth')

if __name__ == '__main__':
    main()
