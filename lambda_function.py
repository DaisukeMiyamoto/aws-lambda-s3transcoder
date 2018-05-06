import boto3
import json
import logging
import os


logger = logging.getLogger()
logger.setLevel(logging.INFO)

def elastictranscoder_create_job(pipeline_id, input_key, output_key,
    thumbnail_key='thumbnails/{resolution}-{count}',
    output_key_prefix='hls/',
    segment_duration = '10',
    region = 'us-west-2'):
    
    # set Preset
    hls_64k_audio_preset_id = '1351620000001-200071'
    # hls_0400k_preset_id     = '1351620000001-200050'
    hls_0400k_preset_id     = '1525586489114-hbzp41'
    hls_0600k_preset_id     = '1351620000001-200040'
    hls_1000k_preset_id     = '1351620000001-200030'
    hls_1500k_preset_id     = '1351620000001-200020'
    hls_2000k_preset_id     = '1351620000001-200010'

    job_input = { 'Key': input_key }
    hls_audio = {
        'Key' : 'hlsAudio/',
        'PresetId' : hls_64k_audio_preset_id,
        'SegmentDuration' : segment_duration
    }
    hls_400k = {
        'Key' : 'hls0400k/',
        'PresetId' : hls_0400k_preset_id,
        'SegmentDuration' : segment_duration,
        'ThumbnailPattern' : thumbnail_key
    }
    hls_600k = {
        'Key' : 'hls0600k/',
        'PresetId' : hls_0600k_preset_id,
        'SegmentDuration' : segment_duration
    }
    hls_1000k = {
        'Key' : 'hls1000k/',
        'PresetId' : hls_1000k_preset_id,
        'SegmentDuration' : segment_duration
    }
    hls_1500k = {
        'Key' : 'hls1500k/',
        'PresetId' : hls_1500k_preset_id,
        'SegmentDuration' : segment_duration
    }
    hls_2000k = {
        'Key' : 'hls2000k/',
        'PresetId' : hls_2000k_preset_id,
        'SegmentDuration' : segment_duration
    }
    # job_outputs = [ hls_audio, hls_400k, hls_600k, hls_1000k, hls_1500k, hls_2000k ]
    job_outputs = [ hls_audio, hls_400k, hls_600k]
    
    # set Playlist
    playlist = {
        'Name' : 'hls_' + output_key,
        'Format' : 'HLSv3',
        'OutputKeys' : list(map(lambda x: x['Key'], job_outputs))
    }
    
    # Create Job
    create_job_request = {
        'PipelineId' : pipeline_id,
        'Input' : job_input,
        'OutputKeyPrefix' : output_key_prefix + output_key +'/',
        'Outputs' : job_outputs,
        'Playlists' : [ playlist ]
    }
    transcoder_client = boto3.client('elastictranscoder')
    create_job_result=transcoder_client.create_job(**create_job_request)
    print('HLS job has been created')
    # print(json.dumps(create_job_result['Job'], indent=4, sort_keys=True)
    
    
def lambda_handler(event, context):
    filename = event['Records'][0]['s3']['object']['key']
    target_name, ext = os.path.splitext(os.path.basename(filename))
    logger.info('Target: ' + str(target_name))

    pipeline_id = os.environ['pipeline_id']
    elastictranscoder_create_job(pipeline_id, 'original/' + target_name + '.mkv', target_name)

    return 'Create Job for %s' % target_name
