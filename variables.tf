variable "project_name" {
  type        = string
  description = "Project name"
}

variable "region" {
  type        = string
  default     = "europe-west1"
  description = "GCP region"
}

variable "ai_notebook_instance_owner" {
  type        = string
  description = "Vertex AI workbench owner"
}

variable "num_worker_nodes" {
  description = "Number of worker nodes in the Dataproc cluster"
  type        = number
  default     = 2
}

variable "worker_machine_type" {
  description = "Dataproc Machine type"
  type        = string
  default     = "n1-standard-8"
}

variable "vertex_machine_type" {
  description = "Vertex Machine type"
  type        = string
  default     = "n1-standard-8"
}