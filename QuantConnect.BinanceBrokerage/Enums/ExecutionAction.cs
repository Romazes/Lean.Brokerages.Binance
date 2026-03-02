/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2026 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

namespace QuantConnect.Brokerages.Binance.Enums
{
    /// <summary>
    /// Describes what action the brokerage should take after parsing a WebSocket execution event.
    /// </summary>
    public enum ExecutionAction
    {
        /// <summary>
        /// No action — routine status update, stop-trigger-consumed EXPIRED, or an unrecognised type.
        /// </summary>
        None,

        /// <summary>
        /// A fill or real-expiry event: forward to <see cref="BinanceBrokerage.OnFillOrder"/>.
        /// </summary>
        Fill,

        /// <summary>
        /// An order was canceled (externally or via REST): forward to <see cref="BinanceBrokerage.OnFillOrder"/>.
        /// <para>
        /// <see cref="BinanceBrokerage.OnFillOrder"/> deduplicates REST-initiated cancels so that
        /// the WS confirmation is dropped when Lean has already emitted the <c>Canceled</c> event.
        /// </para>
        /// </summary>
        Cancel,

        /// <summary>
        /// A <c>NEW</c> order that Lean does not yet know about (placed externally, e.g. Binance Web UI):
        /// forward to <see cref="BinanceBrokerage.OnNewBrokerageOrderNotification"/>.
        /// </summary>
        NewOrder,
    }
}
