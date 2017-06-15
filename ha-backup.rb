#!/usr/bin/env ruby

require 'json'
require 'rest_client'
require 'timeout'
require 'scutil'
require 'stringio'
require 'date'
require 'awesome_print'
require 'logger'

# Backup all instances for tenants in TENANT_MAP to an external SSH
# server.

# Improvements:
#
# 1. Right now, we submit a snapshot request to Nova and wait until
#    it's complete to move to the next one.  The main reason for this
#    is not to overwhelm a compute node with multiple, simultaneous
#    snapshots.  We could be smarter and continue running simultaneous
#    snapshot so long as the instance being snapshotted is on a
#    different compute node.
#
# 2. This script has to be run from the same host that stores the
#    images for glance.
#
# 3. We should do something if an image fails to transfer.  Currently
#    we delete it and pretend that nothing's gone wrong.
#
# 4. Check each instance's state (as reported by Nova) at start and/or
#    before snapshotting.

LOGGER = Logger.new("backup.log")
LOGGER.level = Logger::WARN
KEYSTONE_URI = "http://10.0.4.3:35357/v2.0"
COMPUTE_URI = "http://10.0.4.3:8774/v2"
API_USER = "admin"
API_PASS = "0p3nSt4ck!"
DEFAULT_TENANT = "Admin"
BACKUP_HOST = "ep-2-d.coredial.com"
BACKUP_USER = "ha-backup"
BACKUP_KEY = 
"-----BEGIN DSA PRIVATE KEY-----
MIIBugIBAAKBgQDOr5c9BuH87C1pTNEQ15Q0/bPOWnbF2JiDkmY+L7AaqylGPHtG
Mpjqw/2SJJ0kJMSmO5GkYKErFFRnS1AOIfH8I3kOVqDEPKq0MYSbfF6NIdR8/Cvp
7/+YZECe/79BQxGysmkp5iLuKXw6PHZh7eav6gDoTbNsiW9YUQqZTc/XhwIVANOR
2mQ9sB1AQgnZpeytI76DXzFxAoGAeNuXDNAfL+BjR7GOfC06T4vV901lnH7E6T9w
ZsxrqovLQcd2dsLeoxLxD6ZSFJbV/oNvAHaJVJROiYUM3WbyjAaPDfsK2a42NHkG
+kOoALaczSZkRC4o6HqxW7GtEkKh4Wcwuop0rpqyCiIk1bQ4C237h4jJL8lHVMKl
JY5voz8CgYBlaE2L4Etoml6q8G8LWv+6YNpyTQ8CDkRL/PuAWLvnCXSI4cMFbt4r
dDlkvL4LZo36gclYD+V/75RtLxQn/zwxcbEJCY9g7cjx81D6M/JcHJAa0/07uxb+
CmDxk6z2UELlVje9knfuJ6R3Gg+hoEyIj06jxA0cMYUdoKG/vIx9GAIUTlOKh9+7
tB+2O63SNQCBjYYwkBg=
-----END DSA PRIVATE KEY-----"
# Max seconds to wait for a snapshot to complete.
SNAPSHOT_TIMEOUT = 3600

# A list of tenant prefixes that we want to backup.
TENANT_MAP = [
              "wl",
              "xt",
              "sb1",
              "sb1v",
              "ad"
             ]

# Number of days worth of backups to keep
KEEP_DAYS = 4

# Simple container class for a tenant.  Expects a symbolized JSON
# tenant object, as returned by keystone, as input.
class Tenant
  attr_accessor :prefix, :name, :id
  
  def initialize(json)
    @prefix = json[:description]
    @name = json[:name]
    @id = json[:id]
  end
end

# Simple container class for an instance.  Expects a symbolized JSON
# instance object, as returned by the compute API, as input.
class Instance
  attr_accessor :name, :id, :image_id, :snapshot_name
  
  def initialize(json)
    @name = json[:name]
    @id = json[:id]
    @image_id = nil
    @snapshot_name = nil
  end
end

# Query keystone for an auth token.  Gets a token for DEFAULT_TENANT,
# unless otherwise specified.  Returns an auth token, plus tenant
# info, in JSON.
def get_token(tenant_id=nil)
  if (tenant_id.nil?)
    tenant_key = :tenantName
    tenant_value = DEFAULT_TENANT
  else
    tenant_key = :tenantId
    tenant_value = tenant_id
  end
  begin
    LOGGER.info("Getting Auth Token")
    response = RestClient.post(KEYSTONE_URI + "/tokens",
                               {
                                 auth: {
                                   tenant_key => tenant_value,
                                   passwordCredentials: {
                                     username: API_USER,
                                     password: API_PASS
                                   }
                                 }
                               }.to_json,
                               content_type: :json, accept: :json) # {|response, request, result| p request }
  rescue => e
    ap e
    LOGGER.error(e)
    exit 1
  end
  
  json = JSON.parse(response, symbolize_names: true)
  json[:access][:token]
end

# Query keystone for a list of tenants.  Returns an array of Tenant
# objects.
def get_tenants(auth_token)
  begin
    LOGGER.info("Getting Tenant List")
    response = RestClient.get(KEYSTONE_URI + "/tenants", accept: :json, 'X-Auth-Token' => auth_token)
  rescue => e
    ap e
    LOGGER.error(e)
    exit 1
  end

  json_tenants = JSON.parse(response, symbolize_names: true)[:tenants]
  
  tenants = []
  json_tenants.each do |t|
    tenants << Tenant.new(t) if TENANT_MAP.include? t[:description]
  end
  tenants
end

# Query the compute API for a particular tenant with a resource.
def compute_api_query(resource, tenant_id, auth_token)
  compute_api_query_with_uri(COMPUTE_URI + "/#{tenant_id}#{resource}", auth_token)
end

# Query the compute API with a URI.  Returns a JSON object.
def compute_api_query_with_uri(uri, auth_token)
  begin
    LOGGER.info("Querying Compute API")
    response = RestClient.get(uri, accept: :json, 'X-Auth-Token' => auth_token)
  rescue => e
    LOGGER.error(e)
    ap e
  end
  
  json = JSON.parse(response, symbolize_names: true)
end

# Post an action to the compute API.  Returns the headers as the response.
def compute_api_action(resource, request_body, tenant_id, auth_token)
  begin
    LOGGER.info("Getting API Headers")
    response = RestClient.post(COMPUTE_URI + "/#{tenant_id}/#{resource}", request_body,
                               content_type: :json, accept: :json, 'X-Auth-Token' => auth_token)
  rescue => e
    LOGGER.error(e)
    ap e
  end
  
  response.headers
end

# Backup each instance for a tenant.  Returns a list of image ids to
# be transferred.
def backup_tenant(tenant)
  puts "Backing up tenant: " + tenant.name
  
  # Get an auth token for the current tenant.
  auth_token = get_token(tenant.id)[:id]
  
  # Get a list of all of the instances for this tenant.
  response = compute_api_query( "/servers", tenant.id, auth_token)
  
  instances = []
  # Backup each instance.  Keep a list of images to be copied after.
  response[:servers].each do |s|
    instances << Instance.new(s)
    backup_instance(instances[-1], tenant.id, auth_token)
  end
  instances
end

# Backup an instance.  Returns a list of Instance objects with their
# @image_id field set.
def backup_instance(instance, tenant_id, auth_token)
  timestamp = Time.now.strftime("%Y%m%d%H%M%S")
  
  printf "  %-20s...", instance.name
  STDOUT.flush
  
  instance.snapshot_name = "snapshot-#{instance.name}-#{timestamp}"
  
  # Submit the snapshot request.  Save the image location.
  request_body = {
    createImage: {
      name: "#{instance.snapshot_name}"
    }
  }.to_json
  image_location_uri = compute_api_action("/servers/#{instance.id}/action", request_body, tenant_id, auth_token)[:location]
  
  # Grab the images id.
  image_id = /.+\/(\S+)$/.match(image_location_uri)[1]
  
  # Wait for the job to complete.
  if (poll_for_image(image_location_uri, auth_token))
    puts "success"
    instance.image_id = image_id
  else
    puts "FAILED"
  end
end

# Poll for snapshot job to complete.  Max SNAPSHOT_TIMEOUT seconds.
def poll_for_image(image_location, auth_token)
  response = {}
  begin
    LOGGER.info("Waiting for snapshot to complete.")
    Timeout::timeout(SNAPSHOT_TIMEOUT) do
      response = compute_api_query_with_uri(image_location, auth_token)[:image]
      while (response[:status] != "ACTIVE")
        sleep 5
        response = compute_api_query_with_uri(image_location, auth_token)[:image]
      end
    end
    return true
  rescue Timeout::Error
    LOGGER.error("ERROR: Timeout waiting for image creation.  Last Status: #{response[:status]}.  Last Progress: #{response[:progress]}")
    puts "  Error: Timeout waiting for image creation."
    puts "    Last status was #{response[:status]}"
    puts "    Last progress was #{response[:progress]}"
    return false
  end
end

# Transfer images to a backup server.
def transfer_images(instances)
  puts "Transferring images to #{BACKUP_HOST}"
  total_bytes = 0
  instances.each do |i|
    printf "  %-20s", "#{i.name}..."
    image_size = nil
    begin
      LOGGER.info("Transferring image #{i.name} to #{BACKUP_HOST}")
      Scutil.upload(BACKUP_HOST, BACKUP_USER, "/var/lib/glance/images/#{i.image_id}", "snapshots/#{i.snapshot_name}.img",
                    { key_data: [ BACKUP_KEY ] }) do |ch, name, sent, total|
        image_size = total
      end
      total_bytes += image_size
    rescue Scutil::Error => err
      LOGGER.error(err.message)
      puts "FAILED"
      puts "Error: " + err.message
      LOGGER.error(err.message)
      next
    end
    puts "success"
  end
  total_bytes
end

# Delete the images via OpenStack.
def clean_up_openstack(instances, tenant_id, auth_token)
  puts "Removing snapshots from OpenStack"
  instances.each do |i|
    begin
      LOGGER.info("Removing #{i.name} from OpenStack")
      response = RestClient.delete("#{COMPUTE_URI}/#{tenant_id}/images/#{i.image_id}", 'X-Auth-Token' => auth_token)
    rescue => e
      LOGGER.error(e)
      ap e
      exit 1
    end
  end
end

# Delete anything old than Time.now - days, on the backup host.
def clean_up_backups(days)
  puts "Removing snapshots older than #{days} days from #{BACKUP_HOST}"
    
  output = StringIO.new
  Scutil.exec_command(BACKUP_HOST, BACKUP_USER, "ls snapshots", output,
                      { key_data: [ BACKUP_KEY ] })
  
  files = output.string.split("\n")
  
  files.each do |file|
    timestamp = /snapshot-\S+-(\d+).img$/.match(file)[1]
    date = DateTime.strptime(timestamp, "%Y%m%d")
    if (date <= (DateTime.now - days))
      puts "Removing #{file}..."
      Scutil.exec_command(BACKUP_HOST, BACKUP_USER, "rm snapshots/#{file}", nil,
                          { key_data: [ BACKUP_KEY ] })
    end
  end
end

start_run = Time.now

# Get a new auth token from keystone for subsequent API calls.
token = get_token
admin_auth_token = token[:id]
admin_tenant_id = token[:tenant][:id]

# Get a list of tenants to backup.
tenants = get_tenants(admin_auth_token)

# Backup each tenant.  Take a list of instances to be transferred.
instances = []
tenants.each { |t| instances << backup_tenant(t) }
total_attempted = instances.flatten.count

# Clean up the list.
successful_instances = instances.flatten.select { |i| !i.image_id.nil? }
total_successful = successful_instances.count

# Transfer images.
total_bytes = transfer_images(successful_instances)

# Clean up OpenStack.
clean_up_openstack(successful_instances, admin_tenant_id, admin_auth_token)

# Clean up the backup host.
clean_up_backups(KEEP_DAYS)

end_run = Time.now
duration = end_run - start_run

puts "\n#{total_successful} of #{total_attempted} successfully backed up in #{(duration/60).floor} minutes."
printf "%.1fGB transferred to #{BACKUP_HOST}.\n", total_bytes / 1000000000
LOGGER.info("\n#{total_successful} of #{total_attempted} successfully backed up in #{(duration/60).floor} minutes.")
LOGGER.info("%.1fGB transferred to #{BACKUP_HOST}.\n", total_bytes / 1000000000)
