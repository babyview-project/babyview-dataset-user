"""
This script computes the total number of videos and their cumulative duration in seconds and hours
for all .mp4 files located in the specified directory and its subdirectories.

python /ccn2a/dataset/babyview/babyview-dataset-user/filter_to_release_videos.py

"""


import os
import glob
from tqdm import tqdm
import ray

overall_video_dir = '/data/datasets/babyview/2025.2/gcloud/'
video_files = glob.glob(os.path.join(overall_video_dir, f'**/*.mp4'), recursive=True)

@ray.remote
def get_video_duration(video_file):
    cmd = f"ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 '{video_file}'"
    duration = os.popen(cmd).read().strip()
    try:
        return float(duration)
    except ValueError:
        print(f"Could not get duration for {video_file}, got '{duration}'")
        return 0.0

ray.init(ignore_reinit_error=True, include_dashboard=False, log_to_driver=False)
futures = [get_video_duration.remote(vf) for vf in video_files]
total_video_length = sum(tqdm(ray.get(futures), desc="Calculating total size"))

print(f"Total number of videos: {len(video_files)}")
print(f"Total video length (seconds): {total_video_length}")
print(f"Total video length (hours): {total_video_length / 3600}")

ray.shutdown()