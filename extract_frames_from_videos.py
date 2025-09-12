'''
Script to extract a dataset of image frames from videos


python /ccn2a/dataset/babyview/babyview-dataset-user/extract_frames_from_videos.py \
    --videos_dir "/data/datasets/babyview/2025.2/gcloud" \
    --output_dir "/ccn2a/dataset/babyview/2025.2/extracted_frames_1fps" \
    --debug  \
        
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

def get_args():
    parser = argparse.ArgumentParser(description='Process videos to the desired fps, resolution, rotation.')
    parser.add_argument('--videos_dir', type=str, required=True, help='Path to the directory with videos')
    parser.add_argument('--output_dir', type=str, required=True, help='Path to the directory to save frames')
    parser.add_argument('--num_processes', type=int, default=16, help='Number of parallel processes')
    parser.add_argument('--fps', type=int, default=1, help='FPS to extract frames at')
    parser.add_argument('--debug', action='store_true', help='Debug mode with fewer videos')
    return parser.parse_args()

def extract_frames_from_video(args, video_path):
    output_pattern = '%05d.jpg' # Use JPG, because PNG takes up way too much space
    video_name = os.path.basename(video_path).split('.')[0]
    output_dir = os.path.join(args.output_dir, video_name)
    output_path = os.path.join(output_dir, output_pattern)
    os.makedirs(output_dir, exist_ok=True)

    scale_filter = "scale='if(gte(iw,ih),-1,512):if(gte(iw,ih),512,-1)'" # resize images to 512 on the short side
    cmd = [
        'ffmpeg',
        '-i', video_path,
        '-vf', f'fps={args.fps}:round=near,{scale_filter}',
        '-qscale:v', '1',  # quality scale for JPEG (1 is best, 31 is worst)
        '-pix_fmt', 'yuvj444p', # avoids default 4:2:0 chroma subsampling (which softens color edges).
        '-video_track_timescale', '1000',
        '-frame_pts', '1',  # This names files with the frame PTS value
        '-vsync', 'vfr', # use variable-frame-rate timing: don’t invent or delete frames to force a constant frame rate
        output_path
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        # '-compression_level', '9',  # Max compression
        # '-vf', 'select=eq(pict_type\\,I)',  # Extract I-frames (key frames)
        
    # Count the number of extracted_frames
    # extracted_frames = glob.glob(os.path.join(output_dir, '*.jpg'))
    # duration = float(ffmpeg.probe(video_path)['format']['duration']) # get length of video in seconds
    # print(f"# frames: {len(extracted_frames)}, duration: {duration}, ratio: {duration / len(extracted_frames)}")
    

@ray.remote
def extract_frames_from_video_remote(args, video_file):
    extract_frames_from_video(args, video_file)

def main(args):
    print(args)
    os.makedirs(args.output_dir, exist_ok=True)
    
    
    video_files = glob.glob(os.path.join(args.videos_dir, '**/*.mp4'), recursive=True)
    print(len(video_files))
    
    # just for debugging
    if args.debug:
        video_files = video_files[:3]
        for video_file in tqdm.tqdm(video_files):
            extract_frames_from_video(args, video_file)

    else:
        ray.init(num_cpus=args.num_processes)
        futures = [extract_frames_from_video_remote.remote(args, video_file) for video_file in video_files]
        ray.get(futures)
    

if __name__ == '__main__':
    args = get_args()
    print(args)
    main(args)
    