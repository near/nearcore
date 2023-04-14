# Nearcore Debug UI

## How to Use
Clone nearcore, go to this directory, run `npm install` (only needed for first time), and then
```
npm start
```

This will serve the UI at localhost:3000.

Go to `http://localhost:3000/<RPC address>` to look at the debug UI of a near node.

The RPC address can be either IP:port, or just IP (which will default to port 3030).

## How to deploy in production
Use the included Dockerfile, which will serve the UI on port 80 inside the container.

For example:

```
cd tools/debug-ui
docker build -t nearcore-debug-ui .
docker run -p 8080:80 nearcore-debug-ui
```

## Development

The code is written in TypeScript with the React framework. The one thing most unintuitive about
React is React Hooks (the useState, useMemo, useCallback, useEffect, etc.) Understanding how
hooks work is a **must**: https://reactjs.org/docs/hooks-intro.html

A few less-well-known hooks that are used often in this codebase:

* `useMemo(func, [deps])` (from core React): returns func(), but only recomputing func()if any deps change from the last invocation (by shallow equality of the each dep).

* `useEffect(func, [deps])` (from core React): similar to useMemo, but instead of returning func(),
just executes it, and func() is allowed to have side effects by mutating state (calling setXXX (that
comes from `const [XXX, setXXX] = useState(...);`)).

* `useQuery([keys], () => promise)` (from react-query): returns `{data, error, isLoading}` which
represents fetching some data using the given promise. This is used to render asynchronously fetched
data. While the data is loading, `isLoading` is true; if there is an error, `error` is truthy; and
finally when there is data, `data` is truthy. This can be used to then render each state
accordingly. The keys given to the query are used to memoize the query, so that queries with the
same keys are only fetched once.

It's also helpful to understand at a high level how the react-router library works; this is used
to support deep-linking in the URL (e.g. `/127.0.0.1/cluster` leads to the cluster page), allowing
the UI to be served as a single application.

### Linting & Formatting
The project is configured to use ESLint (error-checking) and Prettier (consistent formatting).

Run `npm run lint` to check for linting & formatting errors, and `npm run fix` to fix those that
can be automatically fixed.

## How to Use the TestLoop Log Visualizer
For nearcore tests written in the TestLoop framework (any tests that mention "TestLoop", or
more precisely, using TestLoop from core/async/src/test_loop.rs), the test can be visualized:

1. First run the test from the Rust side and save the output, e.g.
   ```
   cargo test -p near-chunks test_multi -- --show-output > ~/log.txt
   ```
2. Go to the UI at `/logviz`, such as http://localhost:3000/logviz
3. Drag the log.txt file into the UI.

Screenshots:
<img width="1728" alt="image" src="https://user-images.githubusercontent.com/111538878/222594411-8fa6f8d7-1eaf-42b4-972d-67384e8f2a3f.png">
<img width="1728" alt="image" src="https://user-images.githubusercontent.com/111538878/222594625-76441066-a664-42ff-a7b5-3b3f3c26d1b4.png">
