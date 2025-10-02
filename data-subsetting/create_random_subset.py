'''
Script to create a dataset of 10s clips from raw videos

python /ccn2/u/khaiaw/Code/babyview-pose/babyview-dataset-user/data-subsetting/create_random_subset.py \
    --videos_dir "/ccn2a/dataset/babyview/2025.2/split_10s_clips_256p/main" \
    --output_dir "/ccn2a/dataset/babyview/2025.2/data_subsets/random_132hours/" \
    --num_hours 132 \

'''

import os
import argparse
import subprocess
import ray
import glob
import ffmpeg
import tqdm
from PIL import Image
import multiprocessing
import torchvision
import numpy as np
import torch

def get_args():
    parser = argparse.ArgumentParser(description='Process videos to the desired fps, resolution, rotation.')
    parser.add_argument('--videos_dir', type=str, required=True, help='Path to the directory with videos')
    parser.add_argument('--output_dir', type=str, required=True, help='Path to the directory to save frames')
    parser.add_argument('--num_hours', type=int, default=0, help='Number of hours of video to copy')
    parser.add_argument('--num_processes', type=int, default=8, help='Number of parallel processes')
    return parser.parse_args()

if __name__ == '__main__':
    args = get_args()
    print(args)
    os.makedirs(args.output_dir, exist_ok=True)
    
    video_files = glob.glob(os.path.join(args.videos_dir, '**/*.mp4'), recursive=True)
    
    num_hours = args.num_hours
    seconds_per_clip = 10
    num_videos = num_hours * 3600 // seconds_per_clip
    
    np.random.shuffle(video_files)
    selected_videos = video_files[:num_videos] 
    
    for video_path in tqdm.tqdm(selected_videos, desc="Copying selected videos"):
        # just replace videos_dir with output_dir in the path
        relative_path = os.path.relpath(video_path, args.videos_dir)
        output_path = os.path.join(args.output_dir, relative_path)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        subprocess.run(['cp', video_path, output_path])