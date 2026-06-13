# app/hql/introspect.py

from __future__ import annotations

import inspect


def get_portfolio_schema() -> str:
    """
    Reads the PortfolioQuery class at runtime and returns a structured string
    describing every public method and its docstring.

    Because this uses `inspect` on the live module, any changes to
    portfolio.py are automatically reflected the next time this is called —
    no manual syncing with the LLM prompt needed.

    Returns
    -------
    str
        A formatted schema string ready to inject into an LLM system prompt.
    """
    from app.hql.queries.ticker import TickersQuery, TickerQuery
    from app.hql.queries.portfolio import PortfolioQuery

    lines = [
        "## PortfolioQuery — Available Methods\n",
        "All monetary values are in AED unless stated otherwise.\n",
    ]

    for name, method in inspect.getmembers(TickerQuery, predicate=inspect.isfunction):
        if name.startswith("_"):
            continue  # skip private/dunder methods

        doc = inspect.getdoc(method) or "No docstring."
        sig = inspect.signature(method)

        lines.append(f"### `portfolio.{name}{sig}`")
        lines.append(f"{doc}\n")

    for name, method in inspect.getmembers(TickersQuery, predicate=inspect.isfunction):
        if name.startswith("_"):
            continue  # skip private/dunder methods

        doc = inspect.getdoc(method) or "No docstring."
        sig = inspect.signature(method)

        lines.append(f"### `portfolio.{name}{sig}`")
        lines.append(f"{doc}\n")

    for name, method in inspect.getmembers(
        PortfolioQuery, predicate=inspect.isfunction
    ):
        if name.startswith("_"):
            continue  # skip private/dunder methods

        doc = inspect.getdoc(method) or "No docstring."
        sig = inspect.signature(method)

        lines.append(f"### `portfolio.{name}{sig}`")
        lines.append(f"{doc}\n")

    return "\n".join(lines)
