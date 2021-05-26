using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Akka.Actor;

namespace Akka.Dispatch
{
    public sealed class ChannelSchedulerProvider : ExtensionIdProvider<ChannelTaskScheduler>
    {
        public override ChannelTaskScheduler CreateExtension(ExtendedActorSystem system)
        {
            return new ChannelTaskScheduler(system);
        }
    }

    public sealed class ChannelTaskScheduler : IExtension, IDisposable
    {
        const int WorkInterval = 500;
        const int WorkerStep = 2;

        

        [ThreadStatic]
        private static bool _threadRunning = false;

        [ThreadStatic]
        private static TaskSchedulerPriority _threadPriority = TaskSchedulerPriority.Idle;

        private readonly Task _controlTask;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly Timer _timer;
        private readonly Task[] _coworkers;
        private readonly int _maximumConcurrencyLevel;
        private readonly int _maxWork = 3; //max work items to execute at one priority

        private readonly PriorityTaskScheduler _highScheduler;
        private readonly PriorityTaskScheduler _normalScheduler;
        private readonly PriorityTaskScheduler _lowScheduler;
        private readonly PriorityTaskScheduler _idleScheduler;

        public TaskScheduler High => _highScheduler;
        public TaskScheduler Normal => _normalScheduler;
        public TaskScheduler Low => _lowScheduler;
        public TaskScheduler Idle => _idleScheduler;

        public static ChannelTaskScheduler Get(ActorSystem system)
        {
            return system.WithExtension<ChannelTaskScheduler>(typeof(ChannelSchedulerProvider));
        }

        public ChannelTaskScheduler(ExtendedActorSystem system)
        {
            //todo channel-task-scheduler config section
            var fje = system.Settings.Config.GetConfig("akka.channel-scheduler");
            _maximumConcurrencyLevel = ThreadPoolConfig.ScaledPoolSize(
                        fje.GetInt("parallelism-min"),
                        fje.GetDouble("parallelism-factor", 1.0D), // the scalar-based factor to scale the threadpool size to 
                        fje.GetInt("parallelism-max"));
            _maximumConcurrencyLevel = Math.Max(_maximumConcurrencyLevel, 1);

            _maxWork = fje.GetInt("work-max", _maxWork);
            _maxWork = Math.Max(_maxWork, 3);

            var channelOptions = new UnboundedChannelOptions()
            {
                AllowSynchronousContinuations = true,
                SingleReader = _maximumConcurrencyLevel == 1,
                SingleWriter = false
            };

            _highScheduler = new PriorityTaskScheduler(Channel.CreateUnbounded<Task>(channelOptions), TaskSchedulerPriority.AboveNormal);
            _normalScheduler = new PriorityTaskScheduler(Channel.CreateUnbounded<Task>(channelOptions), TaskSchedulerPriority.Normal);
            _lowScheduler = new PriorityTaskScheduler(Channel.CreateUnbounded<Task>(channelOptions), TaskSchedulerPriority.Low);
            _idleScheduler = new PriorityTaskScheduler(Channel.CreateUnbounded<Task>(channelOptions), TaskSchedulerPriority.Idle);

            _coworkers = new Task[_maximumConcurrencyLevel - 1];
            for (var i = 0; i < _coworkers.Length; i++)
                _coworkers[i] = Task.CompletedTask;

            _timer = new Timer(ScheduleCoWorkers, null, Timeout.Infinite, Timeout.Infinite);

            _controlTask = Task.Run(ControlAsync, _cts.Token);
        }

        public TaskScheduler GetScheduler(TaskSchedulerPriority priority)
        {
            switch (priority)
            {
                case TaskSchedulerPriority.Normal:
                    return _normalScheduler;
                case TaskSchedulerPriority.Realtime:
                case TaskSchedulerPriority.High:
                case TaskSchedulerPriority.AboveNormal:
                    return _highScheduler;
                case TaskSchedulerPriority.BelowNormal:
                case TaskSchedulerPriority.Low:
                    return _lowScheduler;
                case TaskSchedulerPriority.Background:
                    //case TaskSchedulerPriority.Idle:
                    return _idleScheduler;
                default:
                    throw new ArgumentException(nameof(priority));
            }
        }

        //public void QueueTask(Task task, TaskSchedulerPriority priority = TaskSchedulerPriority.Normal)
        //{
        //    switch (priority)
        //    {
        //        case TaskSchedulerPriority.Normal:
        //            _normalScheduler.QueueTask(task);
        //            break;
        //        case TaskSchedulerPriority.Realtime:
        //        case TaskSchedulerPriority.High:
        //        case TaskSchedulerPriority.AboveNormal:
        //            _highScheduler.QueueTask(task);
        //            break;
        //        case TaskSchedulerPriority.BelowNormal:
        //            _lowScheduler.QueueTask(task);
        //            break;
        //        case TaskSchedulerPriority.Background:
        //            //case TaskSchedulerPriority.Idle:
        //            _idleScheduler.QueueTask(task);
        //            break;
        //        default:
        //            throw new ArgumentException(nameof(priority));
        //    }
        //}

        private async Task ControlAsync()
        {
            var highReader = _highScheduler.Channel.Reader;
            var normalReader = _normalScheduler.Channel.Reader;
            var lowReader = _lowScheduler.Channel.Reader;
            var idleReader = _idleScheduler.Channel.Reader;

            var readTasks = new Task<bool>[] {
                highReader.WaitToReadAsync().AsTask(),
                normalReader.WaitToReadAsync().AsTask(),
                lowReader.WaitToReadAsync().AsTask(),
                idleReader.WaitToReadAsync().AsTask()
            };

            Task<bool> readTask;

            do
            {
                //schedule coworkers
                ScheduleCoWorkers(null);

                //main worker
                DoWork();

                //wait on coworker exit
                await Task.WhenAll(_coworkers).ConfigureAwait(false);

                //stop timer
                if (!_cts.IsCancellationRequested)
                    _timer.Change(Timeout.Infinite, Timeout.Infinite);

                readTask = await Task.WhenAny(readTasks).ConfigureAwait(false);

                if(readTasks[0] == readTask)
                    readTasks[0] = highReader.WaitToReadAsync().AsTask();
                else if (readTasks[1] == readTask)
                    readTasks[1] = normalReader.WaitToReadAsync().AsTask();
                else if (readTasks[2] == readTask)
                    readTasks[2] = lowReader.WaitToReadAsync().AsTask();
                else if (readTasks[3] == readTask)
                    readTasks[3] = idleReader.WaitToReadAsync().AsTask();
            }
            while (readTask.Result && !_cts.IsCancellationRequested);
        }

        private void ScheduleCoWorkers(object _)
        {
            var reqWorkerCount = _highScheduler.Channel.Reader.Count
                + _normalScheduler.Channel.Reader.Count
                + _lowScheduler.Channel.Reader.Count
                + _idleScheduler.Channel.Reader.Count;

            //limit req workers
            reqWorkerCount = Math.Min(reqWorkerCount, _maximumConcurrencyLevel);

            //count running workers
            for (var i = 0; reqWorkerCount > 0 && i < _coworkers.Length; i++)
            {
                if (!_coworkers[i].IsCompleted)
                    reqWorkerCount--;
            }

            //limit new workers
            var newWorkerToStart = Math.Min(reqWorkerCount, WorkerStep);
            reqWorkerCount -= newWorkerToStart;

            //start new workers
            for (var i = 0; newWorkerToStart > 0 && i < _coworkers.Length; i++)
            {
                if (_coworkers[i].IsCompleted)
                {
                    _coworkers[i] = Task.Run((Action)Worker, _cts.Token);
                    newWorkerToStart--;
                }
            }

            //reschedule
            if (!_cts.IsCancellationRequested)
            {
                var interval = reqWorkerCount > 0
                    ? WorkInterval / WorkerStep
                    : WorkInterval * WorkerStep;
                _timer.Change(interval, Timeout.Infinite);
            }
        }

        private void Worker()
        {
            DoWork();
        }

        private int DoWork()
        {
            var total = 0;
            int count;

            //maybe implement max work count and/or a deadline

            _threadRunning = true;
            try
            {
                do
                {
                    count = _highScheduler.ExecuteAll();

                    count += _normalScheduler.ExecuteMany(_maxWork);

                    count += count > 0
                        ? _lowScheduler.ExecuteSingle()
                        : _lowScheduler.ExecuteMany(_maxWork);

                    //if there was no work then only execute background tasks 
                    if (count == 0)
                        count += _idleScheduler.ExecuteSingle();

                    total += count;
                }
                while (count > 0 && !_cts.IsCancellationRequested);
            }
            catch
            {
                //ignore error
            }
            finally
            {
                _threadRunning = false;
                _threadPriority = TaskSchedulerPriority.Idle;
            }

            //todo better stats

            return total;
        }

        public void Dispose()
        {
            _idleScheduler.Dispose();
            _lowScheduler.Dispose();
            _normalScheduler.Dispose();
            _highScheduler.Dispose();

            _cts.Cancel();
            _timer.Dispose();
        }

        sealed class PriorityTaskScheduler : TaskScheduler, IDisposable
        {
            readonly Channel<Task> _channel;

            readonly TaskSchedulerPriority _priority;

            public Channel<Task> Channel => _channel;
            //public TaskSchedulerPriority Priority => _priority;

            public PriorityTaskScheduler(Channel<Task> channel, TaskSchedulerPriority priority)
            {
                _channel = channel;
                _priority = priority;
            }

            protected override void QueueTask(Task task)
            {
                if (!_channel.Writer.TryWrite(task))
                    throw new InvalidOperationException();
            }

            protected override IEnumerable<Task> GetScheduledTasks()
            {
                return Array.Empty<Task>();
            }

            protected override bool TryDequeue(Task task)
            {
                return false;
            }

            protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
            {
                // If this thread isn't already processing a task
                // and the thread priority is higher,
                // we don't support inlining
                return (_threadRunning && _threadPriority <= _priority) && TryExecuteTask(task);
            }

            public int ExecuteAll()
            {
                _threadPriority = _priority; //maybe set it in parent

                var reader = _channel.Reader;
                var count = 0;

                while (reader.TryRead(out var task))
                {
                    TryExecuteTask(task);
                    count++; //maybe only count successfully executed
                }
                return count;
            }

            public int ExecuteMany(int maxTasks)
            {
                _threadPriority = _priority; //maybe set it in parent

                var reader = _channel.Reader;
                int c;

                for (c = 0; c < maxTasks && reader.TryRead(out var task); c++)
                    TryExecuteTask(task);

                return c;
            }

            public int ExecuteSingle()
            {
                _threadPriority = _priority; //maybe set it in parent

                if (_channel.Reader.TryRead(out var task))
                {
                    TryExecuteTask(task);
                    return 1;
                }

                return 0;
            }

            public void Dispose()
            {
                _channel.Writer.TryComplete();
            }
        }
    }

    public enum TaskSchedulerPriority
    {
        Idle = 4,
        Background = 4,
        Low = 5,
        BelowNormal = 6,
        Normal = 8,
        AboveNormal = 10,
        High = 13,
        Realtime = 24
    }
}
