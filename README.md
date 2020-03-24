# gmaxfeed

Python3 module for downloading files from gmax server and saving to local machine.

Any problems please let me know via email or comments, I tested it for the first month in Jan 2016 and worked as expected.

Nothing complicated here just might save a bit of time.

Requires an active licence key obtained by purchase from Total Performance Data, http://www.totalperformancedata.com/ .

Feed Specifications below:
- GPS Points: https://www.gmaxequine.com/downloads/GX-RP-00038%202018-10-12%20Gmax%20Race%20Point%20Data%20Feed%20Specification.pdf
- Sectionals: https://www.gmaxequine.com/downloads/GX-RP-00019%202019-02-15%20Gmax%20Race%20Post-Race%20Protocol%20Specification.pdf
- Daily Race List: https://www.gmaxequine.com/downloads/GX-RP-00054%202019-03-07%20Gmax%20Race%20List%20Feed%20Specification.pdf
- Sectionals History: https://www.gmaxequine.com/downloads/GX-RP-00047%202018-12-14%20Gmax%20Race%20Sectionals%20History%20Feed%20Specification.pdf
- Racecourse Surveys: https://www.gmaxequine.com/downloads/GX-RP-00039%202018-02-12%20Gmax%20Race%20Route%20Data%20Feed%20Specification.pdf
- Jumps Locations (Hurdles coming Soon): https://www.gmaxequine.com/downloads/GX-RP-00055%202019-06-05%20Gmax%20Race%20Jumps%20Feed%20Specification.pdf

Live data streams also available are:
- Live GPS Points: https://www.gmaxequine.com/downloads/GX-RP-00007%202018-08-08%20Gmax%20Race%20Live%20Data%20Feed%20Specification.pdf
- Live Progress: https://www.gmaxequine.com/downloads/GX-RP-00020%202019-02-15%20Gmax%20Race%20Live%20Progress%20Feed%20Specification.pdf

Limitations:
- Runners which don't finish are usually not included due to current limitations in the software setup,
- For some courses/distances the 'P' field doesn't decrease, this is a problem in the historic feed mapping to the route files but is usually not an issue in the live feed. We're working on fixing the issue.
- Whip strikes can cause sudden spikes in the data, velocities hitting near 50m/s and skewing the X,Y way off the track, there's nothing we can do about this as the trackers are padded as much as the weight/size guidelines will allow.
- We cannot distribute horse names due to licence limitations, we can only supply the sharecode with saddle cloth number appended.
- If a course code is in the files which is not listed in the FAQ's then it is from a test track and should be disregarded.
