# Exercise 7: From the log to the lake

## Why the example fails

The example job reads and writes, but it never saves where it stopped.  
read_new(None) always starts at the beginning of the topic, so every run copies the whole topic again.  
The first run passes, the lake has all 2 million records once.  
The second run copies the same records again, so the lake ends up with 4 million rows, and every record is in there twice.  
The checker counts that as 2 million duplicates, nothing is missing, but it still fails.

```
partition          topic offsets     in lake  duplicates    missing  not loaded
-------------------------------------------------------------------------------
        0           [0, 493,378)     986,756     493,378          0           0
        1           [0, 461,936)     923,872     461,936          0           0
        2           [0, 522,640)   1,045,280     522,640          0           0
        3           [0, 522,046)   1,044,092     522,046          0           0
-------------------------------------------------------------------------------
    total      2,000,000 records   4,000,000   2,000,000          0           0

Exactly once: FAIL — 2,000,000 duplicates
```

## 1. Decision

I chose architecture B, the lake is the truth.  
The job doesn't use a state file, it gets the position from the lake itself, by taking the highest offset per partition.  
Every run is written into the staging folder first, and when it's finished, publish moves it into the lake with one rename.  
Because of that, the data and the position can't disagree, they are basically the same thing.  
If a run stops before the rename, the lake stays the same, and the next run starts from the same place and clears staging.  
If the rename went through, the whole run is in the lake, so readers never see only half of it.

I didn't go with A, because there the data and the state file have to match, and they get saved in two separate steps.  
When I ran A through check_all, a stop while writing gave around 40 thousand duplicates, and a stop after writing added another 100 thousand.  
A could be fixed with staging, but if the job stops between publish and save_state, the run still gets copied twice, the checker just doesn't test that case.

I didn't go with C either, because it only passes through the view.  
The duplicates are still in the files, after the C test there were about 125 thousand of them.  
Every reader would have to use the view, and removing duplicates on every query makes reading slower.  
C also writes straight into the lake, so readers can see a half written run, after the stop while writing only one partition of the new run was there.

B has some downsides too.  
lake_position reads the highest offset from all the parquet files at the start of every run, so it gets slower as the lake grows, here it didn't really matter.  
A run that stops has to be written again from the start, which costs some time.  
Only one run can work at a time, since write_files clears the staging folder.  
Staging and the lake also have to be on the same file system, otherwise the rename wouldn't work as one step.

## 2. Result

```python
def run_once():
    # 1. where did the last run stop? (from the lake itself)
    start = lake_position()
    # 2. read what is new
    new = read_new(start)
    # 3. write it to staging first, then move it into the lake in one step
    run = write_files(new, STAGING)
    publish(run)
    # 4. nothing to save, the lake already knows where we stopped
```

All checks pass, and the last line says exactly once after every run: YES.  
The first run copies the whole topic, because the lake is empty, that took about 2 minutes.  
When new records arrive, only those get read, so the run takes around 40 seconds.  
When there is nothing new, nothing gets written and the run is done in a couple of seconds.  
When the run stops while writing, part of it is already written, but only in staging, so the lake doesn't change and the new records just show up as not loaded.  
The next run starts from the same position, clears staging and writes the whole run again, without duplicates.  
When the run stops after writing, the files are complete, but the rename never happened, so for the readers that run didn't happen and the next run just does it again.

## 3. Output of check_all

```
situation                                 in lake  duplicates  missing  not loaded    run  check
-----------------------------------------------------------------------------------------------
1 first run                             2,000,000           0        0           0 122.1s  PASS
2 new records arrive, run again         2,100,000           0        0           0  39.3s  PASS
3 nothing new, run again                2,100,000           0        0           0   1.8s  PASS
4 run stops WHILE writing: right after  2,100,000           0        0     100,000         PASS
  ... then the next run                 2,200,000           0        0           0  31.4s  PASS
5 run stops AFTER writing: right after  2,200,000           0        0     100,000         PASS
  ... then the next run                 2,300,000           0        0           0  32.5s  PASS
-----------------------------------------------------------------------------------------------
Exactly once after every run: YES
```

## Extra: A and C

A, state file, writing straight into the lake:

```
situation                                 in lake  duplicates  missing  not loaded    run  check
-----------------------------------------------------------------------------------------------
1 first run                             2,300,000           0        0           0 127.2s  PASS
2 new records arrive, run again         2,400,000           0        0           0  36.2s  PASS
3 nothing new, run again                2,400,000           0        0           0   0.1s  PASS
4 run stops WHILE writing: right after  2,440,474           0        0      59,526         PASS
  ... then the next run                 2,540,474      40,474        0           0  35.9s  FAIL
5 run stops AFTER writing: right after  2,640,474      40,474        0           0         FAIL
  ... then the next run                 2,740,474     140,474        0           0  36.1s  FAIL
-----------------------------------------------------------------------------------------------
Exactly once after every run: NO — see the rows marked FAIL
```

C, same job as A, but checked through the dedup view:

```
situation                                 in lake  duplicates  missing  not loaded    run  check
-----------------------------------------------------------------------------------------------
1 first run                             2,600,000           0        0           0 115.5s  PASS
2 new records arrive, run again         2,700,000           0        0           0  34.5s  PASS
3 nothing new, run again                2,700,000           0        0           0   0.2s  PASS
4 run stops WHILE writing: right after  2,725,196           0        0      74,804         PASS
  ... then the next run                 2,800,000           0        0           0  31.1s  PASS
5 run stops AFTER writing: right after  2,900,000           0        0           0         PASS
  ... then the next run                 2,900,000           0        0           0  33.1s  PASS
-----------------------------------------------------------------------------------------------
Exactly once after every run: YES
```

The notebook is exercise7.ipynb in this folder, section 5 has the A and C runs.
