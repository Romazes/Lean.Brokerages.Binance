/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
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

using System;
using System.Linq;
using Newtonsoft.Json.Linq;
using QuantConnect.Brokerages.Binance.Enums;
using QuantConnect.Brokerages.Binance.Extensions;
using QuantConnect.Brokerages.Binance.Messages;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Securities;

namespace QuantConnect.Brokerages.Binance
{
    public partial class BinanceBrokerage
    {
        private IDataAggregator _aggregator;

        /// <summary>
        /// Locking object for the Ticks list in the data queue handler
        /// </summary>
        protected readonly object TickLocker = new object();

        private void OnUserMessage(WebSocketMessage webSocketMessage)
        {
            var e = (WebSocketClientWrapper.TextMessage)webSocketMessage.Data;

            try
            {
                if (Log.DebuggingEnabled)
                {
                    Log.Debug($"BinanceBrokerage.OnUserMessage(): {e.Message}");
                }

                var obj = JObject.Parse(e.Message);

                var objError = obj["error"];
                if (objError != null)
                {
                    var error = objError.ToObject<ErrorMessage>();
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, error.Code, error.Message));
                    return;
                }

                if (!TryGetExecution(obj, out var execution, out var action))
                {
                    return;
                }

                switch (action)
                {
                    case ExecutionAction.Fill:
                    case ExecutionAction.Cancel:
                        OnFillOrder(execution);
                        break;
                    case ExecutionAction.NewOrder:
                        OnNewBrokerageOrder(execution);
                        break;
                    // ExecutionAction.None: routine update — nothing to do
                }
            }
            catch (Exception exception)
            {
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, -1,
                    $"Parsing wss message failed. Data: {e.Message} Exception: {exception}"));
                throw;
            }
        }

        /// <summary>
        /// Tries to parse a WebSocket JSON payload into an <see cref="Execution"/> and determines
        /// the <see cref="ExecutionAction"/> the brokerage should take.
        /// </summary>
        /// <remarks>
        /// Handles three wire formats:
        /// <list type="number">
        ///   <item><c>{ "e": "executionReport", … }</c> — standard Spot stream</item>
        ///   <item><c>{ "e": "ORDER_TRADE_UPDATE", "o": { … } }</c> — Futures stream</item>
        ///   <item><c>{ "subscriptionId": …, "event": { "e": "executionReport", … } }</c> — Spot WS-API</item>
        /// </list>
        /// Returns <c>false</c> when the payload is not an execution event (e.g. <c>ALGO_UPDATE</c>).
        /// </remarks>
        internal static bool TryGetExecution(JObject payload, out Execution execution, out ExecutionAction action)
        {
            execution = TryExtractExecution(payload);
            if (execution == null)
            {
                action = ExecutionAction.None;
                return false;
            }

            action = ResolveAction(execution);
            return true;
        }

        // ── Wire-format parsing ──────────────────────────────────────────────────────

        /// <summary>
        /// Extracts the <see cref="Execution"/> from whichever of the three Binance wire formats
        /// the payload uses, or returns <c>null</c> when the payload is not an execution event.
        /// </summary>
        private static Execution TryExtractExecution(JObject payload)
        {
            var eventType = payload["e"]?.Value<string>();

            if (eventType == "executionReport")
            {
                return payload.ToObject<Execution>();
            }

            if (eventType == "ORDER_TRADE_UPDATE")
            {
                return payload["o"]?.ToObject<Execution>();
            }

            // Spot WS-API: { "subscriptionId": …, "event": { "e": "executionReport", … } }
            if (payload["event"]?["e"]?.Value<string>() == "executionReport")
            {
                return payload["event"].ToObject<Execution>();
            }

            return null;
        }

        // ── Action routing ───────────────────────────────────────────────────────────

        /// <summary>
        /// Maps the execution's <c>x</c> (execution type) and <c>o</c> (order type) fields to the
        /// action the brokerage should take.
        /// </summary>
        /// <remarks>
        /// Special case: <c>x:EXPIRED, o:STOP|STOP_MARKET</c> → <see cref="ExecutionAction.None"/>
        /// because Binance sends an EXPIRED event when a stop-limit trigger is consumed; the
        /// resulting child order arrives in a separate NEW event and must not be marked Invalid.
        /// </remarks>
        private static ExecutionAction ResolveAction(Execution execution)
        {
            var isStopTrigger =
                execution.OrderType.Equals("STOP", StringComparison.OrdinalIgnoreCase) ||
                execution.OrderType.Equals("STOP_MARKET", StringComparison.OrdinalIgnoreCase);

            return execution.ExecutionType?.ToUpperInvariant() switch
            {
                "TRADE"                      => ExecutionAction.Fill,
                "EXPIRED" when isStopTrigger => ExecutionAction.None,   // trigger consumed — skip
                "EXPIRED"                    => ExecutionAction.Fill,   // real expiry → Invalid
                "NEW"                        => ExecutionAction.NewOrder,
                "CANCELED"                   => ExecutionAction.Cancel,
                _                            => ExecutionAction.None,
            };
        }

        // ── Order event handlers ─────────────────────────────────────────────────────

        /// <summary>
        /// Handles fill, partial-fill, expiry, and cancellation events received over WebSocket.
        /// </summary>
        /// <remarks>
        /// Deduplication: <see cref="CancelOrder"/> (REST) fires <see cref="OnOrderEvent"/> with
        /// <see cref="OrderStatus.Canceled"/> immediately on success.  The subsequent WS
        /// <c>x:CANCELED</c> event is a duplicate and is silently dropped.  Externally-canceled
        /// orders (order still <see cref="OrderStatus.Submitted"/>) propagate correctly.
        /// </remarks>
        internal void OnFillOrder(Execution data)
        {
            try
            {
                var order = GetLeanOrder(data.AlgoOrderId) ?? GetLeanOrder(data.OrderId);
                if (order == null)
                {
                    Log.Error($"BinanceBrokerage.OnFillOrder(): order not found: {data.OrderId} [AlgoOrderId: {data.AlgoOrderId}]");
                    return;
                }

                var status = ConvertOrderStatus(data.OrderStatus);

                // REST CancelOrder already emitted Canceled — the WS confirmation is a duplicate.
                // External cancels arrive with the order in a non-canceled state, so they propagate.
                if (status == OrderStatus.Canceled && order.Status == OrderStatus.Canceled)
                {
                    return;
                }

                var fillPrice    = data.LastExecutedPrice;
                var fillQuantity = data.Direction == OrderDirection.Sell
                    ? -data.LastExecutedQuantity
                    :  data.LastExecutedQuantity;
                var updTime  = Time.UnixMillisecondTimeStampToDateTime(data.TransactionTime);
                var orderFee = string.IsNullOrEmpty(data.FeeCurrency)
                    ? OrderFee.Zero
                    : new OrderFee(new CashAmount(data.Fee, data.FeeCurrency));

                OnOrderEvent(new OrderEvent(
                    order.Id, order.Symbol, updTime, status,
                    data.Direction, fillPrice, fillQuantity,
                    orderFee, $"Binance Order Event {data.Direction}"));
            }
            catch (Exception e)
            {
                Log.Error(e);
                throw;
            }
        }

        /// <summary>
        /// Handles a <c>x:NEW</c> execution event for an order that Lean does not yet track
        /// (i.e. placed externally via the Binance Web UI or another client).
        /// </summary>
        /// <remarks>
        /// Deduplication: if Lean already tracks the order (placed via <see cref="PlaceOrder"/>),
        /// <see cref="GetLeanOrder"/> returns non-null and the method returns early, preventing a
        /// duplicate <see cref="OnNewBrokerageOrderNotification"/> call.
        /// </remarks>
        internal void OnNewBrokerageOrder(Execution execution)
        {
            // Guard: order already known to Lean (placed via PlaceOrder)
            if (GetLeanOrder(execution.AlgoOrderId) != null || GetLeanOrder(execution.OrderId) != null)
            {
                return;
            }

            var openOrder = execution.MapExecutionToOpenOrder();
            if (!TryCreateLeanOrder(openOrder, out var order))
            {
                Log.Error($"BinanceBrokerage.OnNewBrokerageOrder(): unsupported order type " +
                          $"'{execution.OrderType}' for order {execution.OrderId}");
                return;
            }

            OnNewBrokerageOrderNotification(new NewBrokerageOrderNotificationEventArgs(order));

            // Lean assigns order.Id > 0 when it accepts the notification.
            if (order.Id != 0)
            {
                OnOrderEvent(new OrderEvent(
                    order,
                    Time.UnixMillisecondTimeStampToDateTime(execution.TransactionTime),
                    OrderFee.Zero,
                    "Order was submitted outside Lean")
                {
                    Status = OrderStatus.Submitted,
                });
            }
        }

        /// <summary>
        /// Resolves a Binance order ID to a Lean <see cref="Order"/>, or <c>null</c> if the ID is
        /// unknown (empty, <c>"0"</c>, or not tracked by the algorithm).
        /// </summary>
        internal virtual Orders.Order GetLeanOrder(string brokerageOrderId)
        {
            // Binance may send "0" as the order ID for algo orders
            if (string.IsNullOrEmpty(brokerageOrderId) ||
                brokerageOrderId.Equals("0", StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }

            return _algorithm.Transactions.GetOrdersByBrokerageId(brokerageOrderId)?.SingleOrDefault();
        }

        // ── Market-data handlers ─────────────────────────────────────────────────────

        private void OnDataMessage(WebSocketMessage webSocketMessage)
        {
            var e = (WebSocketClientWrapper.TextMessage)webSocketMessage.Data;

            try
            {
                var obj = JObject.Parse(e.Message);

                var objError = obj["error"];
                if (objError != null)
                {
                    var error = objError.ToObject<ErrorMessage>();
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, error.Code, error.Message));
                    return;
                }

                var eventType = obj["e"]?.Value<string>();
                switch (eventType)
                {
                    case "trade":
                    case "aggTrade":
                        var trade = obj.ToObject<Trade>();
                        // Futures feed sends upper and lower case "T" — read it explicitly.
                        trade.Time = obj["T"].ToObject<long>();
                        EmitTradeTick(
                            _symbolMapper.GetLeanSymbol(trade.Symbol, GetSupportedSecurityType(), MarketName),
                            Time.UnixMillisecondTimeStampToDateTime(trade.Time),
                            trade.Price,
                            trade.Quantity);
                        break;

                    case "bookTicker":
                        // Futures streams the event type; Spot does not — handled in the default branch.
                        HandleQuoteTick(obj);
                        break;

                    default:
                        // Spot bookTicker omits "e"; detect by the presence of "u" (update ID).
                        if (obj["u"] != null)
                        {
                            HandleQuoteTick(obj);
                        }
                        break;
                }
            }
            catch (Exception exception)
            {
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, -1,
                    $"Parsing wss message failed. Data: {e.Message} Exception: {exception}"));
                throw;
            }
        }

        private void HandleQuoteTick(JObject objData)
        {
            var quote = objData.ToObject<BestBidAskQuote>();
            EmitQuoteTick(
                _symbolMapper.GetLeanSymbol(quote.Symbol, GetSupportedSecurityType(), MarketName),
                quote.BestBidPrice,
                quote.BestBidSize,
                quote.BestAskPrice,
                quote.BestAskSize);
        }

        private void EmitQuoteTick(Symbol symbol, decimal bidPrice, decimal bidSize, decimal askPrice, decimal askSize)
        {
            var tick = new Tick
            {
                AskPrice = askPrice,
                BidPrice = bidPrice,
                Time     = DateTime.UtcNow,
                Symbol   = symbol,
                TickType = TickType.Quote,
                AskSize  = askSize,
                BidSize  = bidSize,
            };
            tick.SetValue();

            lock (TickLocker)
            {
                _aggregator.Update(tick);
            }
        }

        private void EmitTradeTick(Symbol symbol, DateTime time, decimal price, decimal quantity)
        {
            var tick = new Tick
            {
                Symbol   = symbol,
                Value    = price,
                Quantity = Math.Abs(quantity),
                Time     = time,
                TickType = TickType.Trade,
            };

            lock (TickLocker)
            {
                _aggregator.Update(tick);
            }
        }
    }
}
