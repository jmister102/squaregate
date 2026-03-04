"""Bloomberg API data collection."""

import logging
import pandas as pd

try:
    import blpapi
    BLPAPI_AVAILABLE = True
except ImportError:
    blpapi = None  # type: ignore[assignment]
    BLPAPI_AVAILABLE = False

logger = logging.getLogger(__name__)


class BloombergDataCollector:
    """Collect Bloomberg reference and historical data for a list of tickers."""

    def __init__(self, host: str = 'localhost', port: int = 8194):
        self.host = host
        self.port = port
        self.session = None

    # ── Connection ────────────────────────────────────────────────────────────

    def connect(self) -> bool:
        """Open a Bloomberg API session. Returns True on success."""
        if not BLPAPI_AVAILABLE:
            logger.error("blpapi is not installed. Bloomberg Terminal SDK is required.")
            return False
        try:
            opts = blpapi.SessionOptions()
            opts.setServerHost(self.host)
            opts.setServerPort(self.port)
            self.session = blpapi.Session(opts)

            if not self.session.start():
                logger.error("Bloomberg session failed to start.")
                return False
            if not self.session.openService("//blp/refdata"):
                logger.error("Could not open //blp/refdata service.")
                return False

            logger.info("Connected to Bloomberg API.")
            return True
        except Exception as exc:
            logger.error(f"Connection error: {exc}")
            return False

    def disconnect(self):
        """Stop the Bloomberg session."""
        if self.session:
            self.session.stop()
            logger.info("Disconnected from Bloomberg API.")

    # ── Reference data (BDP) ──────────────────────────────────────────────────

    def get_reference_data(
        self,
        tickers: list,
        fields: list,
        overrides: dict | None = None,
    ) -> pd.DataFrame | None:
        """
        Fetch BDP (reference/point-in-time) data.

        Parameters
        ----------
        tickers : list[str]
            Bloomberg security identifiers, e.g. ['AAPL US Equity'].
        fields : list[str]
            Bloomberg field mnemonics.
        overrides : dict, optional
            Override key/value pairs (e.g. {'CALC_INTERVAL': '100D'}).

        Returns
        -------
        pd.DataFrame with tickers as index, fields as columns, or None on error.
        """
        if not self.session:
            logger.error("No active session — call connect() first.")
            return None

        try:
            svc = self.session.getService("//blp/refdata")
            req = svc.createRequest("ReferenceDataRequest")

            for ticker in tickers:
                req.append("securities", ticker)
            for field in fields:
                req.append("fields", field)

            if overrides:
                ov_elem = req.getElement("overrides")
                for key, val in overrides.items():
                    ov = ov_elem.appendElement()
                    ov.setElement("fieldId", key)
                    ov.setElement("value", str(val))
                logger.debug(f"Overrides applied: {overrides}")

            logger.debug(f"BDP request: {len(tickers)} tickers, {len(fields)} fields")
            self.session.sendRequest(req)

            data: dict = {t: {} for t in tickers}

            while True:
                event = self.session.nextEvent(500)
                if event.eventType() in (
                    blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE
                ):
                    for msg in event:
                        sec_array = msg.getElement("securityData")
                        for i in range(sec_array.numValues()):
                            sec = sec_array.getValueAsElement(i)
                            ticker = sec.getElementAsString("security")
                            field_data = sec.getElement("fieldData")

                            for field in fields:
                                try:
                                    data[ticker][field] = (
                                        field_data.getElement(field).getValue()
                                        if field_data.hasElement(field)
                                        else None
                                    )
                                except Exception as exc:
                                    logger.warning(f"{ticker} / {field}: {exc}")
                                    data[ticker][field] = None

                            if sec.hasElement("fieldExceptions"):
                                for j in range(sec.getElement("fieldExceptions").numValues()):
                                    fe = sec.getElement("fieldExceptions").getValueAsElement(j)
                                    logger.warning(
                                        f"Field exception — {ticker}: "
                                        f"{fe.getElementAsString('fieldId')}"
                                    )

                if event.eventType() == blpapi.Event.RESPONSE:
                    break

            df = pd.DataFrame.from_dict(data, orient='index')
            df.index.name = 'Ticker'
            return df

        except Exception as exc:
            logger.error(f"Reference data error: {exc}")
            return None

    # ── Historical data (BDH) ─────────────────────────────────────────────────

    def get_historical_data(
        self,
        tickers: list,
        fields: list,
        start_date: str,
        end_date: str | None = None,
    ) -> pd.DataFrame | None:
        """
        Fetch BDH (historical) data — returns the last available data point.

        Parameters
        ----------
        tickers : list[str]
        fields : list[str]
        start_date : str
            YYYYMMDD format.
        end_date : str, optional
            YYYYMMDD format; defaults to start_date (single-day lookup).

        Returns
        -------
        pd.DataFrame with tickers as index, or None on error.
        """
        if not self.session:
            logger.error("No active session — call connect() first.")
            return None

        try:
            svc = self.session.getService("//blp/refdata")
            req = svc.createRequest("HistoricalDataRequest")

            for ticker in tickers:
                req.append("securities", ticker)
            for field in fields:
                req.append("fields", field)

            req.set("startDate", start_date)
            req.set("endDate", end_date or start_date)

            logger.debug(f"BDH request: {len(tickers)} tickers, date {start_date}")
            self.session.sendRequest(req)

            data: dict = {t: {} for t in tickers}

            while True:
                event = self.session.nextEvent(500)
                if event.eventType() in (
                    blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE
                ):
                    for msg in event:
                        sec = msg.getElement("securityData")
                        ticker = sec.getElementAsString("security")
                        field_array = sec.getElement("fieldData")

                        if field_array.numValues() > 0:
                            # Use the last (most recent) data point
                            fd = field_array.getValueAsElement(field_array.numValues() - 1)
                            for field in fields:
                                try:
                                    data[ticker][field] = (
                                        fd.getElement(field).getValue()
                                        if fd.hasElement(field)
                                        else None
                                    )
                                except Exception as exc:
                                    logger.warning(f"{ticker} / {field}: {exc}")
                                    data[ticker][field] = None
                        else:
                            for field in fields:
                                data[ticker][field] = None

                if event.eventType() == blpapi.Event.RESPONSE:
                    break

            df = pd.DataFrame.from_dict(data, orient='index')
            df.index.name = 'Ticker'
            return df

        except Exception as exc:
            logger.error(f"Historical data error: {exc}")
            return None
