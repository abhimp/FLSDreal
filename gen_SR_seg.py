import subprocess
import sys
import cv2
# from SuperRes.gen_SR_frames import get_frames
import os

if(len(sys.argv) == 3):
    tmpDir = sys.argv[1]
    lr_stream_id = 0
    sr_stream_id = 7
    seg_id = int(sys.argv[2])
elif(len(sys.argv) == 5):
    tmpDir = sys.argv[1]
    lr_stream_id = int(sys.argv[2])
    sr_stream_id = int(sys.argv[3])
    seg_id = int(sys.argv[4])
else:
    sys.exit(0)

video_LR = os.path.join(tmpDir, 'video_{}_{}.mp4'.format(seg_id, lr_stream_id))
fd_video = open(video_LR,'wb')

init_stream = os.path.join(tmpDir, 'video_init_{}'.format(lr_stream_id))
chunk_stream = os.path.join(tmpDir, 'video_{}_{}'.format(seg_id, lr_stream_id))
subprocess.run(['cat', init_stream, chunk_stream], stdout=fd_video)

fd_video.close()


frames = []
vid_obj = cv2.VideoCapture(video_LR)
while(True):
    ret, f = vid_obj.read()
    if(ret == False):
        break
    # frames.append(cv2.cvtColor(f, cv2.COLOR_BGR2RGB))
    frames.append(f)
vid_obj.release()

print('Done reading frames')
print(len(frames))
print(frames[0].shape)

video_SR = os.path.join(tmpDir, 'videoSR_{}_{}.mp4'.format(seg_id, sr_stream_id))

num = len(frames)
row, col, ch = frames[0].shape


#
# Super Resolution Module to be added
#
outputs = [] 

for i in range(num):
    out_frame = cv2.resize(frames[i], (4*col, 4*row))
    outputs.append(out_frame)

# outputs = get_frames(frames)

print('Done generating frames')

#
#
#



out = cv2.VideoWriter(video_SR, cv2.VideoWriter_fourcc(*'mp4v'), 30.0, (4*col, 4*row))
for i in range(num):
  out.write(outputs[i])
out.release()

print('Done writing video')


seg_SR = os.path.join(tmpDir, 'video_{}_{}.mp4'.format(seg_id, sr_stream_id))
subprocess.run(['ffmpeg','-i',video_SR,'-vcodec','h264','-profile:v','baseline','-movflags','frag_keyframe+empty_moov+default_base_moof',seg_SR])

subprocess.run(['mv', seg_SR, seg_SR[:-4]])
subprocess.run(['rm', video_LR, video_SR])