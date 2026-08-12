#!/usr/bin/python3


import asyncio
import os
from typing import Dict, List, Optional, Set, Tuple, Any, cast
from pathlib import Path
import aiohttp
import requests
import sqlite3


###############################################################################
# Exceptions
###############################################################################


class GitHubAuthError(RuntimeError):
    """
    Raised when the API refuses to give us data. Without this the run happily
    writes a badge full of zeroes, which looks like a successful build and is
    how this repository silently rotted for two months.
    """


###############################################################################
# Main Classes
###############################################################################


class Queries(object):
    """
    Class with functions to query the GitHub GraphQL (v4) API and the REST (v3)
    API. Also includes functions to dynamically generate GraphQL queries.
    """

    def __init__(
        self,
        username: str,
        access_token: str,
        session: aiohttp.ClientSession,
        max_connections: int = 10,
    ):
        self.username = username
        self.access_token = access_token
        self.session = session
        self.semaphore = asyncio.Semaphore(max_connections)

    @staticmethod
    def _raise_for_api_error(result: Any) -> None:
        """
        Turn a dead token or an exhausted rate limit into an exception.

        Both otherwise produce a run that "succeeds" while reporting numbers
        that are zero or quietly truncated. Anything else (a missing
        repository, a field we lack scopes for) is left alone, because those
        are per-field and the rest of the response is still usable.
        """
        if not isinstance(result, dict):
            return

        message = str(result.get("message", ""))
        if "Bad credentials" in message or "Requires authentication" in message:
            raise GitHubAuthError(
                f"GitHub rejected the access token: {message}. "
                "The ACCESS_TOKEN secret has most likely expired -- generate a "
                "new personal access token with 'repo' and 'read:user' scopes."
            )
        if "rate limit" in message.lower():
            raise GitHubAuthError(
                f"GitHub rate limit hit: {message}. Aborting rather than "
                "publishing partial statistics."
            )
        for error in result.get("errors") or []:
            if isinstance(error, dict) and error.get("type") == "RATE_LIMITED":
                raise GitHubAuthError(
                    f"GitHub rate limit hit: {error.get('message')}. Aborting "
                    "rather than publishing partial statistics."
                )

    async def query(self, generated_query: str) -> Dict:
        """
        Make a request to the GraphQL API using the authentication token from
        the environment
        :param generated_query: string query to be sent to the API
        :return: decoded GraphQL JSON output
        """
        headers = {
            "Authorization": f"Bearer {self.access_token}",
        }
        try:
            async with self.semaphore:
                r_async = await self.session.post(
                    "https://api.github.com/graphql",
                    headers=headers,
                    json={"query": generated_query},
                )
            result = await r_async.json()
            if result is not None:
                self._raise_for_api_error(result)
                return result
        except GitHubAuthError:
            raise
        except Exception:
            print("aiohttp failed for GraphQL query")
            # Fall back on non-async requests
            async with self.semaphore:
                r_requests = requests.post(
                    "https://api.github.com/graphql",
                    headers=headers,
                    json={"query": generated_query},
                )
                result = r_requests.json()
                if result is not None:
                    self._raise_for_api_error(result)
                    return result
        return dict()

    async def query_rest(self, path: str, params: Optional[Dict] = None) -> Any:
        """
        Make a request to the REST API
        :param path: API path to query
        :param params: Query parameters to be passed to the API
        :return: deserialized REST JSON output
        """

        for _ in range(60):
            headers = {
                "Authorization": f"token {self.access_token}",
            }
            if params is None:
                params = dict()
            if path.startswith("/"):
                path = path[1:]
            try:
                async with self.semaphore:
                    r_async = await self.session.get(
                        f"https://api.github.com/{path}",
                        headers=headers,
                        params=tuple(params.items()),
                    )
                if r_async.status == 202:
                    # print(f"{path} returned 202. Retrying...")
                    print(f"A path returned 202. Retrying...")
                    await asyncio.sleep(2)
                    continue

                result = await r_async.json()
                if result is not None:
                    self._raise_for_api_error(result)
                    return result
            except GitHubAuthError:
                raise
            except Exception:
                print("aiohttp failed for rest query")
                # Fall back on non-async requests
                async with self.semaphore:
                    r_requests = requests.get(
                        f"https://api.github.com/{path}",
                        headers=headers,
                        params=tuple(params.items()),
                    )
                    if r_requests.status_code == 202:
                        print(f"A path returned 202. Retrying...")
                        await asyncio.sleep(2)
                        continue
                    elif r_requests.status_code == 200:
                        result = r_requests.json()
                        self._raise_for_api_error(result)
                        return result
        # print(f"There were too many 202s. Data for {path} will be incomplete.")
        print("There were too many 202s. Data for this repository will be incomplete.")
        return dict()

    @staticmethod
    def _root(login: Optional[str]) -> str:
        """
        :param login: account to scope a query to, or None for the token owner
        :return: the GraphQL root field to hang a selection off
        """
        return "viewer" if login is None else f'user(login: "{login}")'

    @staticmethod
    def _root_key(login: Optional[str]) -> str:
        """
        :return: the key the root field's payload arrives under in the response
        """
        return "viewer" if login is None else "user"

    @staticmethod
    def identity(login: Optional[str] = None) -> str:
        """
        :param login: account to look up, or None for the token owner
        :return: GraphQL query resolving an account to its stable node ID

        The node ID is the whole point: it survives username changes, so stats
        keyed off it never break when the account is renamed.
        """
        return f"""
query {{
  {Queries._root(login)} {{
    login
    name
    id
  }}
}}
"""

    @staticmethod
    def repos_overview(
        contrib_cursor: Optional[str] = None,
        owned_cursor: Optional[str] = None,
        login: Optional[str] = None,
    ) -> str:
        """
        :return: GraphQL query with overview of user repositories
        """
        return f"""{{
  {Queries._root(login)} {{
    login,
    name,
    repositories(
        first: 100,
        orderBy: {{
            field: UPDATED_AT,
            direction: DESC
        }},
        isFork: false,
        after: {"null" if owned_cursor is None else '"'+ owned_cursor +'"'}
    ) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        nameWithOwner
        stargazers {{
          totalCount
        }}
        forkCount
        languages(first: 10, orderBy: {{field: SIZE, direction: DESC}}) {{
          edges {{
            size
            node {{
              name
              color
            }}
          }}
        }}
      }}
    }}
    repositoriesContributedTo(
        first: 100,
        includeUserRepositories: false,
        orderBy: {{
            field: UPDATED_AT,
            direction: DESC
        }},
        contributionTypes: [
            COMMIT,
            PULL_REQUEST,
            REPOSITORY,
            PULL_REQUEST_REVIEW
        ]
        after: {"null" if contrib_cursor is None else '"'+ contrib_cursor +'"'}
    ) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        nameWithOwner
        stargazers {{
          totalCount
        }}
        forkCount
        languages(first: 10, orderBy: {{field: SIZE, direction: DESC}}) {{
          edges {{
            size
            node {{
              name
              color
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

    @staticmethod
    def contrib_years(login: Optional[str] = None) -> str:
        """
        :return: GraphQL query to get all years the user has been a contributor
        """
        return f"""
query {{
  {Queries._root(login)} {{
    contributionsCollection {{
      contributionYears
    }}
  }}
}}
"""

    @staticmethod
    def contribs_by_year(year: str) -> str:
        """
        :param year: year to query for
        :return: portion of a GraphQL query with desired info for a given year
        """
        return f"""
    year{year}: contributionsCollection(
        from: "{year}-01-01T00:00:00Z",
        to: "{int(year) + 1}-01-01T00:00:00Z"
    ) {{
      contributionCalendar {{
        totalContributions
      }}
    }}
"""

    @classmethod
    def all_contribs(cls, years: List[str], login: Optional[str] = None) -> str:
        """
        :param years: list of years to get contributions for
        :return: query to retrieve contribution information for all user years
        """
        by_years = "\n".join(map(cls.contribs_by_year, years))
        return f"""
query {{
  {cls._root(login)} {{
    {by_years}
  }}
}}
"""

    @staticmethod
    def repo_commits(
        owner: str, name: str, author_id: str, cursor: Optional[str] = None
    ) -> str:
        """
        :param author_id: node ID of the account whose commits we want
        :return: GraphQL query for a page of the author's commits on the
                 default branch, with line counts attached

        Filtering on the author's node ID rather than their login is what makes
        this work across renames: GitHub resolves every historical identity
        (old usernames, old commit emails) back to the same account.
        """
        after = "null" if cursor is None else f'"{cursor}"'
        return f"""
query {{
  repository(owner: "{owner}", name: "{name}") {{
    defaultBranchRef {{
      target {{
        ... on Commit {{
          history(first: 100, after: {after}, author: {{id: "{author_id}"}}) {{
            pageInfo {{
              hasNextPage
              endCursor
            }}
            nodes {{
              oid
              additions
              deletions
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""


class Stats(object):
    """
    Retrieve and store statistics about GitHub usage.
    """

    # Notebooks are excluded from line counts: a single re-executed notebook
    # can dwarf every hand-written line in the account.
    NOTEBOOK_LANGUAGE = "Jupyter Notebook"

    def __init__(
        self,
        username: str,
        access_token: str,
        session: aiohttp.ClientSession,
        exclude_repos: Optional[Set] = None,
        exclude_langs: Optional[Set] = None,
        ignore_forked_repos: bool = False,
        extra_logins: Optional[List[str]] = None,
    ):
        self.username = username
        self._ignore_forked_repos = ignore_forked_repos
        self._exclude_repos = set() if exclude_repos is None else exclude_repos
        self._exclude_langs = set() if exclude_langs is None else exclude_langs
        self._extra_logins = list(extra_logins) if extra_logins else []
        self.queries = Queries(username, access_token, session)
        self.db_path = Path('stats_cache.db')
        self._init_db()

        self._name: Optional[str] = None
        self._stargazers: Optional[int] = None
        self._forks: Optional[int] = None
        self._total_contributions: Optional[int] = None
        self._languages: Optional[Dict[str, Any]] = None
        self._repos: Optional[Set[str]] = None
        self._lines_changed: Optional[Tuple[int, int]] = None
        self._views: Optional[int] = None
        self._identities: Optional[List[Dict[str, Optional[str]]]] = None

    def _init_db(self):
        self.conn = sqlite3.connect(self.db_path)
        # Commits are cached by SHA alone. The previous schema keyed on
        # (repo, sha), so renaming the account invalidated the entire cache.
        existing = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='commit_lines'"
        ).fetchone()
        if existing:
            columns = [
                row[1]
                for row in self.conn.execute("PRAGMA table_info(commit_lines)")
            ]
            primary_key = [
                row[1]
                for row in self.conn.execute("PRAGMA table_info(commit_lines)")
                if row[5]
            ]
            if "repo" in columns or primary_key != ["sha"]:
                self.conn.execute(
                    "CREATE TABLE IF NOT EXISTS commit_lines_v2 ("
                    "sha TEXT PRIMARY KEY, additions INTEGER, deletions INTEGER)"
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO commit_lines_v2 (sha, additions, deletions) "
                    "SELECT sha, additions, deletions FROM commit_lines"
                )
                self.conn.execute("DROP TABLE commit_lines")
                self.conn.execute(
                    "ALTER TABLE commit_lines_v2 RENAME TO commit_lines"
                )
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS commit_lines (
                sha TEXT PRIMARY KEY,
                additions INTEGER,
                deletions INTEGER
            )
        """)
        # The old per-repo aggregate cache was keyed on the repository's full
        # name and was never invalidated, so it froze line counts in place.
        # Repository totals are now recomputed each run; they are cheap.
        self.conn.execute("DROP TABLE IF EXISTS repo_lines")
        self.conn.commit()

    async def to_str(self) -> str:
        """
        :return: summary of all available statistics
        """
        languages = await self.languages_proportional
        formatted_languages = "\n  - ".join(
            [f"{k}: {v:0.4f}%" for k, v in languages.items()]
        )
        lines_changed = await self.lines_changed
        identities = await self.identities
        formatted_identities = ", ".join(
            cast(str, i.get("login")) for i in identities
        )
        return f"""Name: {await self.name}
Accounts counted: {formatted_identities}
Stargazers: {await self.stargazers:,}
Forks: {await self.forks:,}
All-time contributions: {await self.total_contributions:,}
Repositories with contributions: {len(await self.repos)}
Lines of code added: {lines_changed[0]:,}
Lines of code deleted: {lines_changed[1]:,}
Lines of code changed: {lines_changed[0] + lines_changed[1]:,}
Project page views: {await self.views:,}
Languages:
  - {formatted_languages}"""

    @property
    async def identities(self) -> List[Dict[str, Optional[str]]]:
        """
        :return: every account whose activity should be counted, as dicts of
                 login/name/id

        The token owner is always included. Any logins in EXTRA_LOGINS that
        resolve to the same account (i.e. are former names of it) are reported
        and skipped, since GitHub already folds their history into the account.
        """
        if self._identities is not None:
            return self._identities

        result = await self.queries.query(Queries.identity())
        viewer = (result.get("data") or {}).get("viewer") or {}
        if not viewer.get("id"):
            raise GitHubAuthError(
                "GitHub returned no account for the access token. The token is "
                "missing, expired, or lacks the 'read:user' scope."
            )

        identities: List[Dict[str, Optional[str]]] = [
            {
                "login": viewer.get("login"),
                "name": viewer.get("name"),
                "id": viewer.get("id"),
            }
        ]
        seen = {viewer.get("id")}

        for login in self._extra_logins:
            result = await self.queries.query(Queries.identity(login))
            user = (result.get("data") or {}).get("user") or {}
            if not user.get("id"):
                print(
                    f"Warning: '{login}' does not resolve to a GitHub account "
                    "and will be skipped. If it is a former username, its "
                    "history is already counted under the current account."
                )
                continue
            if user.get("id") in seen:
                print(
                    f"'{login}' is the same account as "
                    f"'{viewer.get('login')}' -- its history is already "
                    "included, so it will not be counted twice."
                )
                continue
            seen.add(user.get("id"))
            identities.append(
                {
                    "login": user.get("login"),
                    "name": user.get("name"),
                    "id": user.get("id"),
                }
            )

        self._identities = identities
        return self._identities

    async def get_stats(self) -> None:
        """
        Get lots of summary statistics using one big query. Sets many attributes
        """
        self._stargazers = 0
        self._forks = 0
        self._languages = dict()
        self._repos = set()

        exclude_langs_lower = {x.lower() for x in self._exclude_langs}

        identities = await self.identities
        # The token owner is queried through `viewer` so private repositories
        # are included; any additional accounts are queried by login.
        logins: List[Optional[str]] = [None] + [
            cast(str, i["login"]) for i in identities[1:]
        ]

        for login in logins:
            root_key = Queries._root_key(login)
            next_owned = None
            next_contrib = None
            while True:
                raw_results = await self.queries.query(
                    Queries.repos_overview(
                        owned_cursor=next_owned,
                        contrib_cursor=next_contrib,
                        login=login,
                    )
                )
                raw_results = raw_results if raw_results is not None else {}
                root = (raw_results.get("data") or {}).get(root_key) or {}

                if login is None:
                    self._name = root.get("name") or root.get("login")

                contrib_repos = root.get("repositoriesContributedTo") or {}
                owned_repos = root.get("repositories") or {}

                repos = owned_repos.get("nodes") or []
                if not self._ignore_forked_repos:
                    repos += contrib_repos.get("nodes") or []

                for repo in repos:
                    if repo is None:
                        continue
                    name = repo.get("nameWithOwner")
                    if name in self._repos or name in self._exclude_repos:
                        continue
                    self._repos.add(name)
                    self._stargazers += repo.get("stargazers", {}).get("totalCount", 0)
                    self._forks += repo.get("forkCount", 0)

                    for lang in repo.get("languages", {}).get("edges", []):
                        name = lang.get("node", {}).get("name", "Other")
                        languages = await self.languages
                        if name.lower() in exclude_langs_lower:
                            continue
                        if name in languages:
                            languages[name]["size"] += lang.get("size", 0)
                            languages[name]["occurrences"] += 1
                        else:
                            languages[name] = {
                                "size": lang.get("size", 0),
                                "occurrences": 1,
                                "color": lang.get("node", {}).get("color"),
                            }

                if owned_repos.get("pageInfo", {}).get(
                    "hasNextPage", False
                ) or contrib_repos.get("pageInfo", {}).get("hasNextPage", False):
                    next_owned = owned_repos.get("pageInfo", {}).get(
                        "endCursor", next_owned
                    )
                    next_contrib = contrib_repos.get("pageInfo", {}).get(
                        "endCursor", next_contrib
                    )
                else:
                    break

        if not self._name:
            raise GitHubAuthError(
                "GitHub returned no profile for the access token -- refusing to "
                "generate a badge full of zeroes."
            )
        if not self._repos:
            raise GitHubAuthError(
                "GitHub returned no repositories for the access token. Check "
                "that it has the 'repo' scope."
            )

        # TODO: Improve languages to scale by number of contributions to
        #       specific filetypes
        excluded_langs = {self.NOTEBOOK_LANGUAGE}
        for lang in excluded_langs:
            try:
                self._languages.pop(lang)
            except KeyError:
                pass
        langs_total = sum([v.get("size", 0) for v in self._languages.values()])
        for v in self._languages.values():
            v["prop"] = 100 * (v.get("size", 0) / langs_total) if langs_total else 0

    @property
    async def name(self) -> str:
        """
        :return: GitHub user's name (e.g., Jacob Strieb)
        """
        if self._name is not None:
            return self._name
        await self.get_stats()
        assert self._name is not None
        return self._name

    @property
    async def stargazers(self) -> int:
        """
        :return: total number of stargazers on user's repos
        """
        if self._stargazers is not None:
            return self._stargazers
        await self.get_stats()
        assert self._stargazers is not None
        return self._stargazers

    @property
    async def forks(self) -> int:
        """
        :return: total number of forks on user's repos
        """
        if self._forks is not None:
            return self._forks
        await self.get_stats()
        assert self._forks is not None
        return self._forks

    @property
    async def languages(self) -> Dict:
        """
        :return: summary of languages used by the user
        """
        if self._languages is not None:
            return self._languages
        await self.get_stats()
        assert self._languages is not None
        return self._languages

    @property
    async def languages_proportional(self) -> Dict:
        """
        :return: summary of languages used by the user, with proportional usage
        """
        if self._languages is None:
            await self.get_stats()
            assert self._languages is not None

        return {k: v.get("prop", 0) for (k, v) in self._languages.items()}

    @property
    async def repos(self) -> Set[str]:
        """
        :return: list of names of user's repos
        """
        if self._repos is not None:
            return self._repos
        await self.get_stats()
        assert self._repos is not None
        return self._repos

    @property
    async def total_contributions(self) -> int:
        """
        :return: count of user's total contributions as defined by GitHub
        """
        if self._total_contributions is not None:
            return self._total_contributions

        identities = await self.identities
        logins: List[Optional[str]] = [None] + [
            cast(str, i["login"]) for i in identities[1:]
        ]

        self._total_contributions = 0
        for login in logins:
            root_key = Queries._root_key(login)
            years = (
                ((await self.queries.query(Queries.contrib_years(login))).get("data") or {})
                .get(root_key, {})
                .get("contributionsCollection", {})
                .get("contributionYears", [])
            )
            if not years:
                continue
            by_year = (
                ((await self.queries.query(Queries.all_contribs(years, login))).get("data") or {})
                .get(root_key, {})
                .values()
            )
            for year in by_year:
                self._total_contributions += year.get("contributionCalendar", {}).get(
                    "totalContributions", 0
                )
        return cast(int, self._total_contributions)

    async def _commit_lines_excluding_notebooks(
        self, repo: str, sha: str, fallback: Tuple[int, int]
    ) -> Tuple[int, int]:
        """
        Line changes for a single commit with .ipynb files subtracted out.

        :param fallback: the unfiltered counts, returned if the REST call
                         cannot tell us which files changed. Falling back beats
                         returning zero, which is indistinguishable from a
                         commit that only touched notebooks.
        """
        cur = self.conn.execute(
            "SELECT additions, deletions FROM commit_lines WHERE sha=?", (sha,)
        )
        row = cur.fetchone()
        if row:  # already cached; commit SHAs are immutable so this never stales
            return row

        commit_data = await self.queries.query_rest(f"/repos/{repo}/commits/{sha}")
        files = commit_data.get("files") if isinstance(commit_data, dict) else None
        if not files:
            return fallback

        additions, deletions = 0, 0
        for f in files:
            if f.get("filename", "").endswith(".ipynb"):
                continue
            additions += f.get("additions", 0)
            deletions += f.get("deletions", 0)

        self.conn.execute(
            "INSERT OR REPLACE INTO commit_lines (sha, additions, deletions) "
            "VALUES (?, ?, ?)",
            (sha, additions, deletions),
        )
        self.conn.commit()
        return additions, deletions

    async def _compute_repo_lines(self, repo: str) -> Tuple[int, int]:
        """
        Compute additions/deletions the user authored in a repository.

        Commits are collected over GraphQL, filtered by author node ID, which
        catches every former username and commit email belonging to the
        account. This replaces filtering the REST commits endpoint by
        `?author=<login>`, which silently returns nothing for commits made
        under a previous username.
        """
        owner, _, name = repo.partition("/")
        if not owner or not name:
            return 0, 0

        commits: Dict[str, Tuple[int, int]] = dict()
        for identity in await self.identities:
            cursor = None
            while True:
                result = await self.queries.query(
                    Queries.repo_commits(
                        owner, name, cast(str, identity["id"]), cursor
                    )
                )
                repository = (result.get("data") or {}).get("repository") or {}
                branch = repository.get("defaultBranchRef") or {}
                target = branch.get("target") or {}
                history = target.get("history") or {}

                for node in history.get("nodes") or []:
                    oid = node.get("oid")
                    if not oid:
                        continue
                    # Keyed by SHA, so a commit reachable from more than one
                    # identity is only counted once
                    commits[oid] = (
                        node.get("additions", 0) or 0,
                        node.get("deletions", 0) or 0,
                    )

                page_info = history.get("pageInfo") or {}
                if page_info.get("hasNextPage"):
                    cursor = page_info.get("endCursor")
                else:
                    break

        # Every commit is checked for notebooks via its file list, which is the
        # only place that information exists. Deciding this per repository -- on
        # whether GitHub currently reports Jupyter Notebook among its languages
        # -- is not sound: a repo whose notebooks were deleted, or replaced with
        # Jupytext .py files, no longer reports the language while its history
        # is still full of them. One such repository inflated the total here by
        # over 800,000 lines. Results are cached by SHA, so this is a one-off
        # cost per commit rather than a per-run cost.
        # Issued concurrently; Queries.semaphore caps how many are actually in
        # flight. Awaiting them one at a time left that budget unused and made
        # the uncached first run roughly ten times slower than it needed to be.
        counted = await asyncio.gather(
            *(
                self._commit_lines_excluding_notebooks(repo, sha, counts)
                for sha, counts in commits.items()
            )
        )
        additions = sum(a for a, _ in counted)
        deletions = sum(d for _, d in counted)
        return additions, deletions

    @property
    async def lines_changed(self) -> Tuple[int, int]:
        """
        :return: count of total lines added, removed, or modified by the user
        """
        if self._lines_changed is not None:
            return self._lines_changed
        additions = 0
        deletions = 0
        for repo in await self.repos:
            repo_adds, repo_dels = await self._compute_repo_lines(repo)
            additions += repo_adds
            deletions += repo_dels

        self._lines_changed = (additions, deletions)
        return self._lines_changed

    @property
    async def views(self) -> int:
        """
        Note: only returns views for the last 14 days (as-per GitHub API)
        :return: total number of page views the user's projects have received
        """
        if self._views is not None:
            return self._views

        total = 0
        for repo in await self.repos:
            r = await self.queries.query_rest(f"/repos/{repo}/traffic/views")
            if not isinstance(r, dict):
                continue
            for view in r.get("views", []):
                total += view.get("count", 0)

        self._views = total
        return total


###############################################################################
# Main Function
###############################################################################


async def main() -> None:
    """
    Used mostly for testing; this module is not usually run standalone
    """
    access_token = os.getenv("ACCESS_TOKEN")
    user = os.getenv("GITHUB_ACTOR")
    if access_token is None or user is None:
        raise RuntimeError(
            "ACCESS_TOKEN and GITHUB_ACTOR environment variables cannot be None!"
        )
    extra_logins_raw = os.getenv("EXTRA_LOGINS")
    extra_logins = (
        [x.strip() for x in extra_logins_raw.split(",") if x.strip()]
        if extra_logins_raw
        else None
    )
    async with aiohttp.ClientSession() as session:
        s = Stats(user, access_token, session, extra_logins=extra_logins)
        print(await s.to_str())


if __name__ == "__main__":
    asyncio.run(main())
