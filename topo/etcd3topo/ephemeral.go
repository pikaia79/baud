package etcd3topo

import (
    "context"
    "github.com/coreos/etcd/clientv3"
    "time"
    "github.com/tiglabs/baudengine/topo"
)

func (s *Server) CreateUniqueEphemeral(ctx context.Context, cell string, filePath string, contents []byte,
        timeout time.Duration) (topo.Version, error) {
    leaseId, err := s.newLease(ctx, cell, timeout)
    if err != nil {
        return nil, err
    }

    _, version, err := s.newUniqueEphemeral(ctx, cell, leaseId, filePath, string(contents))
    if err != nil {
        return nil, err
    }

    return EtcdVersion(version), nil
}

func (s *Server) newLease(ctx context.Context, cell string, timeout time.Duration) (clientv3.LeaseID, error){
    c, err := s.clientForCell(ctx, cell)
    if err != nil {
        return 0, err
    }

    // Get a lease, set its KeepAlive.
    lease, err := c.cli.Grant(ctx, int64(timeout / time.Second))
    if err != nil {
        return 0, convertError(err)
    }
    leaseKA, err := c.cli.KeepAlive(ctx, lease.ID)
    if err != nil {
        return 0, convertError(err)
    }
    go func() {
        for range leaseKA {
        }
    }()

    return lease.ID, nil
}

// newUniqueEphemeralKV creates a new file in the provided directory.
// It is linked to the Lease.
// Errors returned are converted to topo errors.
func (s *Server) newUniqueEphemeral(ctx context.Context, cell string, leaseId clientv3.LeaseID, newKey string,
        contents string) (string, int64, error) {
    c, err := s.clientForCell(ctx, cell)
    if err != nil {
        return "", 0, err
    }

    // Only create a new file if it doesn't exist already
    // (version = 0), to avoid two processes using the
    // same file name. Since we use the lease ID, this should never happen.
    txnresp, err := c.cli.Txn(ctx).
        If(clientv3.Compare(clientv3.Version(newKey), "=", 0)).
        Then(clientv3.OpPut(newKey, contents, clientv3.WithLease(leaseId))).
        Commit()
    if err != nil {
        if err == context.Canceled || err == context.DeadlineExceeded {
            // Our context was canceled as we were sending
            // a creation request. We don't know if it
            // succeeded or not. In any case, let's try to
            // delete the node, so we don't leave an orphan
            // node behind for *leaseTTL time.
            c.cli.Delete(context.Background(), newKey)
        }
        return "", 0, convertError(err)
    }
    if !txnresp.Succeeded {
        // The key already exists, that should not happen.
        return "", 0, ErrBadResponse
    }
    // The key was created.
    return newKey, txnresp.Header.Revision, nil
}

