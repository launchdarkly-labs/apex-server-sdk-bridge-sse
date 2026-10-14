# Flag demo

A small Visualforce page that evaluates a flag with the SDK and shows the result, for
checking that the bridge is delivering flag data to an org.

It is a separate Salesforce DX project rather than part of `force-app`, so that deploying
the SDK never deploys the demo.

## What it shows

- One featured flag, evaluated for a user key you choose, with the SDK's evaluation reason
  and variation index. The flag is read with `jsonVariation`, so any flag type works.
- Every flag the bridge has mirrored into the org, with its value for the same user.

Each evaluation writes a feature event to `EventData__c`, which the bridge drains and
forwards to LaunchDarkly, so reloading the page exercises both directions.

## Deploying

Deploy the SDK from the repository root first, then the demo from this directory:

```bash
sf project deploy start --source-dir ../../force-app --target-org <org>
sf project deploy start --source-dir force-app --target-org <org>
```

Then open `/apex/FlagDemo` on the org's My Domain host. The page accepts two optional
parameters, `user` and `flag`:

```
https://MyDomainName.my.salesforce.com/apex/FlagDemo?user=demo-user&flag=test-flag
```

With the bridge streaming, a change saved in LaunchDarkly shows on the next reload.

## A note on the controller

The evaluation runs from the page's `action` attribute, not from the controller's
constructor. Every evaluation writes an event row, and Visualforce forbids DML during
construction; the first draft of this page failed with "DML currently not allowed" for
exactly that reason.
