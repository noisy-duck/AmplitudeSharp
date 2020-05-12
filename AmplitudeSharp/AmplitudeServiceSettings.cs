using System;
using System.Collections.Generic;
using System.Text;

namespace AmplitudeSharp
{
	public class AmplitudeServiceSettings
	{
		/// <summary>
		/// The number of seconds to wait before retry if the Amplitude API returns an error or throttles us.
		/// Default value of 30s recommended by Amplitude docs.
		/// </summary>
		public uint BackOffDelaySeconds = 30;

		/// <summary>
		/// The number of seconds to wait after receiving an event before we dispatch it. This allows the
		/// service to group multiple calls in quick succession together. Recommended 0 for desktop and 10s
		/// for mobile devices.
		/// </summary>
		public uint DispatchBatchPeriodSeconds = 0;

		/// <summary>
		/// The maximum amount of time a call can remain in the event queue (includes events that have been
		/// persisted after a session). Max recommended time is 7 days as after that we can't guarantee an
		/// event is unique if it get's replayed (insert_id validity is 7 days for events API).
		/// </summary>
		public uint QueuedApiCallsTTLSeconds = 60 * 60 * 24 * 7;

		/// <summary>
		/// The time period between background saves of the event queue. Ensures we persist any outstanding
		/// events in the event of a crash, or on environments where we may not have full control of the
		/// application lifecycle. Will not write if there is no new data to write. Setting to 0 will disable
		/// background writes (the persistence store will still be used on startup / exit).
		/// </summary>
		public uint BackgroundWritePeriodSeconds = 3;

		public AmplitudeServiceSettings()
		{
			// We use some different sensible defaults if we are a mobile device
			var device = new DeviceHelper();
			if(device.IsMobile)
			{
				DispatchBatchPeriodSeconds = 10;
			}
		}
	}
}
