# TranscodeMapReduce
Sample code for a very simple and limited video transcoding MapReduce job.
Transcodes to and from HDFS.
Currently it doesn't support audio, and there are missing frames at the beginning of the input split, because it is not using the actual keyframes, but the raw HDFS block.
Also destination container is hardwired at Matroska and it is rescaling to a hardwired 640x480.
Framerate seems to be a little off too, and also some artifacts: Maybe we should be using PTS instead of DTS as part of the reducer key pair.
