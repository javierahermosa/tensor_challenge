# org.tensor.challenge.Tensor Challenge

The challenge consisted of computing the decayed sum at given triples of
half-lifes = {1e6, 1e9, 1e12}, reset times = {60s, 1 min, 1 hour}, and 
reset ticks = {1000, 1000000}, for one day of tick data of a single
stock.

## The Data

A quick look at the data shows that there are a few challenges:
1. The timestamps are not smoothly monotonically increasing, showing a few
gaps over the course of the day. When computing the time elapsed since 
the first record, I see that don't allow me to compute the reset_time at 
all requested values give: there is a jump from a fraction of a second to 
over an hour.
2. There are several entries with dissimilar values for a single timestamp, 
which I de-duplicated by selecting the most complete record (larger number 
of volumes) for a given timestamp.
3. There are not enough records to compute the decayed sum at 1,000,000. 
Removing the null values already yields less records than this number and 
de-duplicating the records reduces the number of records to less than 700k.

Upon reading the file, I pre-processed the data as mentioned above in the 
class IO:
a. Removed rows containing only null volumes.
b. De-duplicated records that had the same timestamp but dissimilar volume 
data, keeping the most complete record (typically five volumes).

## The Method

Once the data was pre-processed as mentioned above, I created various 
helper columns that would allow me to compute time differences and the 
records for which the the decayed-sum had to be reset. The main method 
is called org.tensor.challenge.Tensor.

Then, I used a user-defined aggregate function org.tensor.challenge.CalculateDecayedSum in 
orden to compute the sum for the Bid and Ask volumes, separately for 
clarity. Creating this class allows me to use the decayed-sum value 
from the previous column, aggregate cumulatively, and reset the sum 
if the reset conditions are met.

The output or result of the program is a dataframe showing the timestamp, 
tick, half-life, volumes, elapsed time in nanoseconds, the decayed sums 
for the respective volumes, and the reason for reset (time or 
tick expiration). 


## Run

In order to run the program, use the Java 1.8 SDK, clone the repository,
place the challenge.json file on the resources folder and run the 
class org.tensor.challenge.Tensor.

To run tests  on the main functions of the program, sun the class TensorTest

## Performance 

In developing the code, it was important for me to avoid performing 
too many aggregation operations, e.g. running a window function 
on volume columns one at the time. Using a custom aggregation function 
allowed me to compute all decayed sums over one iteration. I did use 
a couple of additional window functions to determine the times 
I which I had to report or reset the sum. 

The data is also small enough to fit in memory, so I persisted input 
DataFrame upon reading it. In a more scalable version usign e.g. streaming, 
this may not be necessary if the chunks are small enough. 

In my machine, the code takes 18.2 seconds to run using four threads.





