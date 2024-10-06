ARG PYTHON_VER

FROM python:${PYTHON_VER:?} AS python-base
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]
ENV PIP_DISABLE_PIP_VERSION_CHECK=1


FROM python-base AS poetry
RUN --mount=type=cache,target=/root/.cache pip install poetry
RUN python -m venv /venv
ENV VIRTUAL_ENV=/venv \
    PATH="/venv/bin:$PATH"
RUN poetry config virtualenvs.create false
WORKDIR /workspace
COPY pyproject.toml poetry.lock /workspace/

# Poetry needs these to exist to setup the editable install
RUN mkdir -p src/denokv && touch src/denokv/__init__.py README.md
RUN --mount=type=cache,target=/root/.cache poetry install


FROM poetry AS test
RUN --mount=source=.,target=/workspace,rw \
    --mount=type=cache,uid=1000,target=.pytest_cache \
    --mount=type=cache,uid=1000,target=.hypothesis \
    pytest


FROM poetry AS lint-setup
# invalidate cache so that the lint tasks run. We use no-cache-filter here but
# not on the lint-* tasks so that the tasks can mount cache dirs themselves.
RUN touch .now


FROM lint-setup AS lint-check
RUN --mount=source=.,target=/workspace,rw \
    ruff check src stubs test


FROM lint-setup AS lint-format
RUN --mount=source=.,target=/workspace,rw \
    ruff format --check --diff src stubs test


FROM lint-setup AS lint-mypy
RUN --mount=source=.,target=/workspace,rw \
    --mount=type=cache,target=.mypy_cache \
    mypy src stubs test


FROM poetry AS smoketest-pkg-build
RUN --mount=source=testing/smoketest,target=.,rw \
  mkdir /dist && poetry build -o /dist


FROM scratch AS smoketest-pkg
COPY --from=smoketest-pkg-build /dist/* .


FROM poetry AS denokv-pkg-build
RUN --mount=source=.,target=/workspace,rw \
  mkdir /dist && poetry build -o /dist


FROM scratch AS denokv-pkg
COPY --from=denokv-pkg-build /dist/* .


FROM scratch AS denokv-bin
COPY --from=ghcr.io/denoland/denokv:latest /usr/local/bin/denokv /denokv


FROM python-base AS test-package-install
COPY --from=denokv-bin /denokv /usr/local/bin/denokv
RUN python -m venv /env
ENV PATH=/env/bin:$PATH
RUN --mount=from=smoketest-pkg,target=/pkg/smoketest \
    --mount=from=denokv-pkg,target=/pkg/v8serialize \
    --mount=type=cache,target=/root/.cache \
  pip install /pkg/smoketest/*.whl /pkg/v8serialize/*.whl


FROM test-package-install AS test-package
RUN pip list
RUN denokv-python-smoketest
