### LibDPR Preview

You can find a preview of our DPR library here. libDPR can be found under the 'cs/libdpr'. You can find the docs under 'docs/_pages/'. Note that this branch is still under active development, and we will
release libDPR more formally when it is ready. For a more detailed writeup about the DPR model and the guarantees, please checkout
[our SIGMOD 2021 paper](https://tli2.github.io/assets/pdf/dpr-sigmod2021.pdf). If you are here for the source code we used for said paper, you can find
those [here](https://github.com/tli2/FASTER/commits/serverless) for D-FASTER and [here](https://github.com/tli2/FASTER/commits/libdpr) for D-Redis.

The current version includes a small example of how you might use DPR to enhance a storage system. You can find the sample under `cs/libdpr/sample`. You can look at `Program.cs` to see how we start
a simple DPR cluster on one machine. You can also run it directly, as we have hardcoded a few simple operations against the cluster. For a more detailed walkthrough, please
look under 'docs/_pages/'.
