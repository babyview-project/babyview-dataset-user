
# -i skip existing files, regardless of modification time
# -n dry run

# -r recursive 
# -x exclude files
# -d delete files in destination that are not in source
# gsutil rsync \
#     gs://babyview_bing_storage babyview_bing_storage \
#     -r \
#     -x ".*\.MP4$" \
#     -d \
#     -n
#     gs://babyview_main_storage babyview_main_storage \

    
#     -n \
#     -i \

# gcloud storage command below is the same as the gsutil above
# gcloud additionally allows a --skip-if-dest-has-newer-mtime flag
cd /data/datasets/babyview/2025.2/gcloud/


    # gs://babyview_main_storage babyview_main_storage \

gcloud storage rsync \
    gs://babyview_bing_storage babyview_bing_storage \
    --recursive \
    --dry-run \
    --delete-unmatched-destination-objects \

    # --exclude=".*\.MP4$" \
    # --skip-if-dest-has-newer-mtime \

'''
#NOTE: --delete-unmatched-destination-objects and --skip-if-dest-has-newer-mtime CANNOT be run together, it will delete files that have newer mtime in destination
    --checksums-only
'''

chmod 777 -R babyview_main_storage/ babyview_bing_storage/ 