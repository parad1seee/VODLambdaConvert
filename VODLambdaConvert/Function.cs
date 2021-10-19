using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.MediaConvert;
using Amazon.MediaConvert.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace VODLambdaConvert
{
    public class Function
    {
        private async Task<string> ListEndpoint(RegionEndpoint region, ILambdaContext context)
        {
            try
            {
                var config = new AmazonMediaConvertConfig { RegionEndpoint = region };
                var client = new AmazonMediaConvertClient(config);
                var request = new DescribeEndpointsRequest();
                var response = await client.DescribeEndpointsAsync(request);

                return response.Endpoints.FirstOrDefault()?.Url;
            }
            catch (Exception ex)
            {
                context.Logger.LogLine(ex.InnerException?.Message ?? ex.Message);
                return "Error";
            }
        }

        public async Task FunctionHandler(S3Event s3Event, ILambdaContext context)
        {
            try
            {
                var destinationS3Bucket = Environment.GetEnvironmentVariable("DestinationBucket");
                var mediaConvertRole = Environment.GetEnvironmentVariable("MediaConvertRole");
                var region = Environment.GetEnvironmentVariable("Region");
                var regionEndpoint = RegionEndpoint.GetBySystemName(region);
                var mediaConvertEndpoint = await ListEndpoint(regionEndpoint, context);
                var jobRequestsTaskList = new List<Task<CreateJobResponse>>();

                var config = new AmazonMediaConvertConfig { ServiceURL = mediaConvertEndpoint };
                var client = new AmazonMediaConvertClient(config);

                foreach (var record in s3Event.Records)
                {
                    var s3Bucket = record.S3.Bucket.Name;
                    var inputFile = record.S3.Object.Key;
                    var s3InputUri = $"s3://{s3Bucket}/{inputFile}";

                    var input = new Input()
                    {
                        FileInput = s3InputUri,
                        AudioSelectors = new Dictionary<string, AudioSelector>
                        {
                            {
                                "Audio Selector 1", new AudioSelector
                                {
                                    Offset = 0,
                                    DefaultSelection = AudioDefaultSelection.DEFAULT,
                                    ProgramSelection = 1
                                }
                            }
                        },
                        VideoSelector = new VideoSelector { ColorSpace = ColorSpace.FOLLOW },
                        FilterEnable = InputFilterEnable.AUTO,
                        PsiControl = InputPsiControl.USE_PSI,
                        DeblockFilter = InputDeblockFilter.DISABLED,
                        DenoiseFilter = InputDenoiseFilter.DISABLED,
                        TimecodeSource = InputTimecodeSource.EMBEDDED,
                        FilterStrength = 0
                    };

                    var outputGroup = new OutputGroup
                    {
                        Name = "File Group",
                        OutputGroupSettings = new OutputGroupSettings
                        {
                            Type = OutputGroupType.HLS_GROUP_SETTINGS,
                            //FileGroupSettings = new FileGroupSettings { Destination = $"s3://{destinationS3Bucket}/outputs/" }
                            HlsGroupSettings = new HlsGroupSettings 
                            { 
                                Destination = $"s3://{destinationS3Bucket}/outputs/",
                                SegmentLength = 10,
                                MinSegmentLength = 5
                            }
                        },
                        Outputs = new List<Output>
                            {
                                //new Output
                                //{
                                //    Preset = "System-Generic_Hd_Mp4_Avc_Aac_16x9_Sdr_1280x720p_30Hz_5Mbps_Qvbr_Vq9",
                                //    Extension = "mp4",
                                //    NameModifier = "_Generic720",
                                //    ContainerSettings = new ContainerSettings
                                //    {
                                //        Container = "MP4",
                                //        Mp4Settings = new Mp4Settings
                                //        {
                                //            CslgAtom = Mp4CslgAtom.INCLUDE,
                                //            FreeSpaceBox = Mp4FreeSpaceBox.EXCLUDE,
                                //            MoovPlacement = Mp4MoovPlacement.PROGRESSIVE_DOWNLOAD
                                //        }
                                //    },
                                //},
                                //new Output
                                //{
                                //    Preset = "System-Generic_Hd_Mp4_Avc_Aac_16x9_1920x1080p_60Hz_9Mbps",
                                //    Extension = "mp4",
                                //    NameModifier = "_Generic1080"
                                //},
                                new Output
                                {
                                    Preset = "System-Avc_16x9_1080p_29_97fps_8500kbps",
                                    Extension = "hls",
                                    NameModifier = "_HLS1080"
                                }
                            }
                    };

                    var jobSettings = new JobSettings
                    {
                        AdAvailOffset = 0,
                        Inputs = new List<Input> { input },
                        OutputGroups = new List<OutputGroup> { outputGroup }
                    };

                    var request = new CreateJobRequest
                    {
                        Settings = jobSettings,
                        Role = mediaConvertRole
                    };

                    jobRequestsTaskList.Add(client.CreateJobAsync(request));
                }

                await Task.WhenAll(jobRequestsTaskList);
            }
            catch (Exception ex)
            {
                context.Logger.LogLine(ex.Message);
            }
        }
    }
}
