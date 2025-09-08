'''
Script to create a dataset of 10s clips from raw videos

pip freeze | grep ffmpeg:
ffmpeg==1.4
ffmpeg-python==0.2.0
imageio-ffmpeg==0.6.0

python /ccn2a/dataset/babyview/babyview-dataset-user/split_into_10s_clips.py \
    --videos_dir "/data/datasets/babyview/2025.2/gcloud/" \
    --output_dir "/ccn2a/dataset/babyview/2025.2/split_10s_clips_256p" \
        
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

debug = False
video_output_resolution = 256

def get_args():
    parser = argparse.ArgumentParser(description='Process videos to the desired fps, resolution, rotation.')
    parser.add_argument('--videos_dir', type=str, required=True, help='Path to the directory with videos')
    parser.add_argument('--output_dir', type=str, required=True, help='Path to the directory to save frames')
    parser.add_argument('--num_processes', type=int, default=8, help='Number of parallel processes')
    return parser.parse_args()

def create_10second_videos(args, video_path):
    if 'bing' in video_path.lower():
        output_dir = os.path.join(args.output_dir, 'bing')
    elif 'main' in video_path.lower():
        output_dir = os.path.join(args.output_dir, 'main')
    else:
        output_dir = os.path.join(args.output_dir, 'other')
        
    video_name = os.path.basename(video_path).split('.')[0]
    output_dir = os.path.join(output_dir, video_name)
    os.makedirs(output_dir, exist_ok=True)
    output_pattern = os.path.join(output_dir, f"{video_name}_%03d.mp4")

    try:
        probe = ffmpeg.probe(video_path)
        video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
        if video_stream:
            width = int(video_stream['width'])
            height = int(video_stream['height'])
            if width > height:
                print(f"Note: Video {video_name} is horizontal (resolution: {width}x{height}).")


        inp = ffmpeg.input(video_path)
        v = (
            inp.video
            .filter('fps', '30')  # CFR 30
            .filter(
                'scale',
                f"if(gt(a,1),-2,{video_output_resolution})",
                f"if(gt(a,1),{video_output_resolution},-2)"
            )
        )
        a = inp.audio

        (
            ffmpeg
            .output(
                v, a,                               # pass BOTH streams
                output_pattern,
                f='segment',
                segment_time=10,
                reset_timestamps=1,                 # or 0 if you want absolute PTS
                vcodec='libx264',                   # or libopenh264 / hardware encoder
                acodec='aac',                       # re-encode audio to AAC
                r=30, g=300,
                force_key_frames='expr:gte(t,n_forced*10)',
                **{'b:a': '192k', 'ar': 48000}      # optional: control audio quality
            )
            .run(quiet=True, overwrite_output=True)
        )

    except Exception as e:
        print(f"Error processing video {video_path}: {e}")


    
@ray.remote(num_gpus=0.25)
def create_10second_videos_remote(args, video_file):
    # Reserve approximately 200 MB of GPU memory by allocating a tensor.
    create_10second_videos(args, video_file)

def main(args):
    print(args)
    os.makedirs(args.output_dir, exist_ok=True)
    
    video_files = glob.glob(os.path.join(args.videos_dir, '**/*.mp4'), recursive=True)
    video_files.sort()
    print(f"Total number of video files: {len(video_files)}")

    if debug:
        video_files = video_files[:10]
        for video_file in tqdm.tqdm(video_files):
            create_10second_videos(args, video_file)

    else:
        ray.init(num_cpus=args.num_processes)
        futures = [create_10second_videos_remote.remote(args, video_file) for video_file in video_files]
        ray.get(futures)
    

if __name__ == '__main__':
    args = get_args()
    print(args)
    main(args)
    