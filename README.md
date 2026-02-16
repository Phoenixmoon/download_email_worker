# Download Email Worker


This repository contains the Worker Lambda function, which is part of a larger system designed to download, clean, and embed emails for the purposes of a email-based RAG system intended to help retrieve relevant emails and summarize them in response to queries. The download process is initiated by a Commander Lambda, which orchestrates batches of email UIDs for processing by fanning out to this Worker Lambda.

## Architecture Overview

This system leverages two primary AWS Lambda functions to efficiently process emails:

1.  **Commander Lambda**: This function is responsible for fetching a list of email UIDs from a specified email folder. It then batches these UIDs and asynchronously invokes the `email_worker_docker` (this repository's Lambda) for each batch.
    * **GitHub Repository:** [Link to Commander Lambda GitHub Repo](https://github.com/your-org/commander-lambda-repo)

2.  **Worker Lambda (`email_worker_docker` - This Repository)**: Triggered by the Commander Lambda, this function is designed to:
    * Connect to the Gmail IMAP server using provided credentials.
    * Download individual email messages based on the UIDs received in its payload.
    * Clean and embed the downloaded emails to then be uploaded to the zilliz cloud vector database. 

    [//]: # (document speed and performance later)