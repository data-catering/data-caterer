
<script src="index.js"></script>

## What does it look like

Play around with the example below to see how Data Caterer can generate transaction data, validate it and then clean
it up.
<p style="font-size: small">*This is for illustrative purposes</p>

<span class="center-content">
<a id="generate-button" class="md-button md-button--primary button-spaced">Generate</a>
<a id="validate-button" class="md-button md-button--primary button-spaced">Validate</a>
<a id="cleanup-button" class="md-button md-button--primary button-spaced">Cleanup</a>
</span>

<div class="center-content" style="flex-direction: column">
    <div id="raw-transaction-container" class="raw-transaction-container"></div>
    <img src="diagrams/index/basic_demo_data_consumer.svg"
    alt="Data consumer reading from source and pushing to sink data source">
    <div id="result-container">
        <div id="transaction-card-container" class="all-card-container"></div>
        <div id="validation-container"></div>
    </div>
</div>