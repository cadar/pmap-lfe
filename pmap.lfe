(defmodule pmap
  (export (start 0)))

;; helpers
(defun num-zip (li)
  (: lists zip (: lists seq 1 (length li)) li))

(defun num-unzip (li)
  (flet ((unpack (x) (let (((tuple num res) x)) res)))
    (: lists map (fun unpack 1) (: lists sort li))))

;; pmap
(defun apply-repack (f x)
  (let* (((tuple num arg) x)
	 (res (apply f (list arg)))
	 (re-num (tuple num res))
	 (mess (tuple (self) re-num)))
    mess))

(defun pmap-link (f l)
  (let* ((parent (self))
	 (raw (lc ((<- pid (lc ((<- x (num-zip l)))
			     (spawn_link
			      (lambda ()
				(! parent (apply-repack f x)))))))
		(receive
		  ((tuple pid result) result)))))
    (num-unzip raw)))

(defun spawn-eval (f-maplink)
  (let* ((parent (self))
	 ((tuple pid ref)
	  (spawn_monitor
	   (lambda ()
	     (let ((result (apply f-maplink '())))
	     (! parent (tuple (self) result)))))))
    (receive
      ((tuple 'DOWN ref _ _ 'normal)
       (receive
	 ((tuple pid result) result)))
      ((tuple 'DOWN ref _ _ reason)
       (tuple 'error reason)))))

(defun pmap (f l)
  (spawn-eval (lambda () (pmap-link f l))))

(defun start ()
  (let ((result (pmap (lambda (x) 
				(let ((sleep-ms (- 1000 (* 10 x)))
				      (result (* x 2)))
				  (: timer sleep sleep-ms)
				  (: io format '"~p~n" (list result))
				  result))
			      '(1 2 3 4 5 6 5 4 3 2 1))))
    (: io format '"~p~n" (list result))))
